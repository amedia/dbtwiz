import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

from ..config.project import project_config
from ..dbt.manifest import Manifest
from ..integrations.bigquery import BigQueryClient
from ..utils.logger import error, fatal, info, warn
from .descriptions import _expand_bq_connection_pool

_MAX_WORKERS = 32


def _collect_datasets(manifest: dict) -> Set[str]:
    """Return the project.dataset of every model, seed and snapshot that dbt materializes."""
    datasets = set()
    for node in manifest["nodes"].values():
        if node["resource_type"] not in ("model", "seed", "snapshot"):
            continue
        if node["config"].get("materialized") == "ephemeral":
            continue
        datasets.add(f"{node['database']}.{node['schema']}")
    return datasets


def _split_by_project(
    dataset_ids: Set[str], projects: List[str]
) -> Tuple[List[str], List[str]]:
    """Split project.dataset ids into (in the given projects, elsewhere), both sorted."""
    included = sorted(d for d in dataset_ids if d.split(".", 1)[0] in projects)
    excluded = sorted(d for d in dataset_ids if d.split(".", 1)[0] not in projects)
    return included, excluded


def _missing_labels(current: Dict[str, str], desired: Dict[str, str]) -> Dict[str, str]:
    """Return the desired labels the dataset does not have yet.

    A label the dataset already has is left alone, also when its value differs.
    """
    return {key: value for key, value in desired.items() if key not in current}


def _conflicting_labels(
    current: Dict[str, str], desired: Dict[str, str]
) -> Dict[str, str]:
    """Return {key: current value} for desired labels the dataset has with another value."""
    return {
        key: current[key]
        for key, value in desired.items()
        if key in current and current[key] != value
    }


def _fetch_dataset(bq_client: Any, dataset_id: str, not_found_exc: type):
    """Fetch a single BQ dataset, returning (dataset_id, dataset_or_none, error_or_none)."""
    try:
        return dataset_id, bq_client.get_dataset(dataset_id), None
    except not_found_exc:
        return dataset_id, None, "not_found"
    except Exception as e:
        return dataset_id, None, e


def _build_update_list(
    bq_client: Any,
    dataset_ids: List[str],
    desired: Dict[str, str],
    not_found_exc: type,
    forbidden_exc: type,
) -> list:
    """Fetch the datasets in parallel and return the ones missing any desired label.

    Stops the command on a permission error, since it applies to every dataset alike.
    """
    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
        futures = [
            executor.submit(_fetch_dataset, bq_client, dataset_id, not_found_exc)
            for dataset_id in dataset_ids
        ]
        results = sorted(
            (future.result() for future in as_completed(futures)), key=lambda r: r[0]
        )

    for dataset_id, _, err in results:
        if isinstance(err, forbidden_exc):
            fatal(f"Not allowed to read dataset {dataset_id}", exception=err)

    updates = []
    for dataset_id, dataset, err in results:
        if err == "not_found":
            warn(f"Dataset not found in BigQuery, skipping: {dataset_id}")
            continue
        if err is not None:
            error(f"Failed to fetch dataset {dataset_id}", exception=err)
            continue

        current = dataset.labels or {}
        for key, value in _conflicting_labels(current, desired).items():
            warn(
                f"Dataset {dataset_id} has label {key}={value}, "
                f"not {key}={desired[key]}; leaving it unchanged"
            )
        missing = _missing_labels(current, desired)
        if missing:
            updates.append(
                {"dataset_id": dataset_id, "dataset": dataset, "missing": missing}
            )
    return updates


def _apply_update(bq_client: Any, item: dict, stop: threading.Event):
    """Add the missing labels to a single dataset, keeping its other labels.

    Returns (item, error_or_none), or (item, "skipped") once `stop` is set.
    """
    if stop.is_set():
        return item, "skipped"
    try:
        dataset = item["dataset"]
        dataset.labels = {**(dataset.labels or {}), **item["missing"]}
        bq_client.update_dataset(dataset, ["labels"])
        return item, None
    except Exception as e:
        return item, e


def _apply_updates(bq_client: Any, updates: list, forbidden_exc: type) -> None:
    """Apply the updates in parallel, stopping at the first permission error.

    Exits with an error when any update failed.
    """
    stop = threading.Event()
    updated, failed = [], []
    forbidden = None
    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
        futures = [
            executor.submit(_apply_update, bq_client, item, stop) for item in updates
        ]
        for future in as_completed(futures):
            item, err = future.result()
            if err is None:
                updated.append(item)
            elif err == "skipped":
                continue
            elif isinstance(err, forbidden_exc):
                stop.set()
                forbidden = forbidden or (item["dataset_id"], err)
            else:
                failed.append(item["dataset_id"])
                error(f"Failed to update {item['dataset_id']}", exception=err)

    for item in sorted(updated, key=lambda i: i["dataset_id"]):
        labels = ", ".join(f"{k}={v}" for k, v in item["missing"].items())
        info(f"- {item['dataset_id']}  ({labels})", style="bold")
    info(
        f"Added labels to {len(updated)} of {len(updates)} dataset(s).",
        style="green" if len(updated) == len(updates) else "yellow",
    )

    if forbidden:
        dataset_id, err = forbidden
        fatal(
            f"Not allowed to update dataset {dataset_id}; "
            "stopped the remaining updates",
            exception=err,
        )
    if failed:
        fatal(f"Failed to update {len(failed)} dataset(s): {', '.join(sorted(failed))}")


def add_dataset_labels(
    dry_run: bool = True,
    manifest_path: Path = Manifest.PROD_MANIFEST_PATH,
    impersonate: bool = False,
) -> None:
    """Add the configured dataset labels to the datasets of the models in a manifest."""
    config = project_config()
    desired = config.dataset_labels
    if not desired or not config.dataset_labels_projects:
        info(
            "dataset_labels or dataset_labels_projects is not configured in "
            "pyproject.toml; nothing to add."
        )
        return

    if manifest_path == Manifest.PROD_MANIFEST_PATH:
        Manifest.download_prod_manifest(force=True)
    manifest = Manifest.get_manifest(manifest_path)

    dataset_ids, other_dataset_ids = _split_by_project(
        _collect_datasets(manifest), config.dataset_labels_projects
    )
    if other_dataset_ids:
        info(
            f"Skipping {len(other_dataset_ids)} dataset(s) outside "
            f"dataset_labels_projects: {', '.join(other_dataset_ids)}"
        )
    if not dataset_ids:
        info("No datasets found in dataset_labels_projects.")
        return

    client = BigQueryClient(
        default_project=config.service_account_project,
        impersonation_service_account=config.service_account_identifier
        if impersonate
        else None,
    )
    bq_client = client.get_client()
    _expand_bq_connection_pool(bq_client)

    info(f"Checking labels on {len(dataset_ids)} dataset(s)...")
    updates = _build_update_list(
        bq_client, dataset_ids, desired, client.NotFound, client.Forbidden
    )
    if not updates:
        info("All datasets already have the configured labels.")
        return

    if dry_run:
        info(
            f"Dry-run: {len(updates)} dataset(s) are missing labels "
            "(use --apply to add them):",
            style="yellow",
        )
        for item in updates:
            labels = ", ".join(f"{k}={v}" for k, v in item["missing"].items())
            info(f"- {item['dataset_id']}  ({labels})", style="bold")
        return

    info(f"Adding labels to {len(updates)} dataset(s)...")
    _apply_updates(bq_client, updates, client.Forbidden)
