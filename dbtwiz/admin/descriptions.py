from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ..config.project import project_config
from ..dbt.manifest import Manifest
from ..integrations.bigquery import BigQueryClient
from ..utils.logger import error, info, warn

_MAX_WORKERS = 32
_AUGMENTED_START = "[comment]: <> (START AUGMENTED DOCS)"
_AUGMENTED_END = "[comment]: <> (END AUGMENTED DOCS)"


def _strip_augmented_docs(description: str) -> str:
    """Strip the deploy-augmented section from a description, keeping only the dbt-authored text."""
    if not description or _AUGMENTED_START not in description:
        return description
    start = description.find(_AUGMENTED_START)
    end = description.find(_AUGMENTED_END)
    if end == -1:
        return description[:start].strip()
    return description[end + len(_AUGMENTED_END) :].strip()


def _expand_bq_connection_pool(native_client: Any) -> None:
    """Expand the HTTP connection pool so parallel threads aren't throttled."""
    from requests.adapters import HTTPAdapter

    adapter = HTTPAdapter(pool_connections=128, pool_maxsize=128, max_retries=3)
    native_client._http.mount("https://", adapter)
    native_client._http._auth_request.session.mount("https://", adapter)


def _get_manifest_columns(node: dict) -> Dict[str, str]:
    """Extract lowercased column name → description mapping from a manifest node."""
    return {
        col_name.lower(): col_data.get("description", "") or ""
        for col_name, col_data in node.get("columns", {}).items()
    }


def _flatten_schema_descriptions(fields, prefix: str = "") -> Dict[str, str]:
    """Flatten a BQ schema to {lower_full_column_name: description} for easy comparison."""
    result = {}
    for field in fields:
        full_name = f"{prefix}{field.name.lower()}"
        result[full_name] = field.description or ""
        if field.field_type == "RECORD" and field.fields:
            result.update(
                _flatten_schema_descriptions(field.fields, prefix=f"{full_name}.")
            )
    return result


def _rebuild_schema_with_descriptions(
    fields, manifest_columns: Dict[str, str], prefix: str = ""
):
    """Recursively rebuild a BQ schema, updating descriptions for columns present in the manifest.

    Columns not present in manifest_columns keep their existing description unchanged.
    """
    from google.cloud import bigquery

    new_fields = []
    for field in fields:
        full_name = f"{prefix}{field.name.lower()}"
        repr_dict = field.to_api_repr()

        if full_name in manifest_columns:
            repr_dict["description"] = manifest_columns[full_name]

        if field.field_type == "RECORD" and field.fields:
            nested = _rebuild_schema_with_descriptions(
                field.fields, manifest_columns, prefix=f"{full_name}."
            )
            # Reuse the already-computed repr_dict from each nested field
            repr_dict["fields"] = [f._properties for f in nested]

        new_fields.append(bigquery.SchemaField.from_api_repr(repr_dict))

    return new_fields


def _find_column_changes(
    bq_schema, manifest_columns: Dict[str, str]
) -> List[Tuple[str, str, str]]:
    """Return list of (column, current_description, desired_description) for changed columns."""
    bq_flat = _flatten_schema_descriptions(bq_schema)
    changes = []
    for col_name, desired_desc in manifest_columns.items():
        col_lower = col_name.lower()
        if col_lower in bq_flat:
            current_desc = bq_flat[col_lower]
            if current_desc != (desired_desc or ""):
                changes.append((col_name, current_desc, desired_desc or ""))
    return changes


def _fetch_table(bq_client: Any, table_id: str, not_found_exc: type):
    """Fetch a single BQ table, returning (table_id, table_or_none, error_or_none)."""
    try:
        return table_id, bq_client.get_table(table_id), None
    except not_found_exc:
        return table_id, None, "not_found"
    except Exception as e:
        return table_id, None, e


def _collect_nodes(manifest: dict, model_names: Optional[List[str]]) -> Dict[str, dict]:
    """Return {table_id: node} for all materialized models and snapshots in the manifest."""
    result = {}
    for node in manifest["nodes"].values():
        if node["resource_type"] not in ("model", "snapshot"):
            continue
        if node["config"].get("materialized") == "ephemeral":
            continue
        name = node["name"]
        if model_names and name not in model_names:
            continue
        table_id = f"{node['database']}.{node['schema']}.{node.get('alias') or name}"
        result[table_id] = node
    return result


def _fetch_bq_tables(
    bq_client: Any, nodes_by_table_id: Dict[str, dict], not_found_exc: type
) -> Dict[str, Any]:
    """Fetch all tables in parallel, logging warnings/errors for failures."""
    bq_tables: Dict[str, Any] = {}
    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
        futures = {
            executor.submit(_fetch_table, bq_client, tid, not_found_exc): tid
            for tid in nodes_by_table_id
        }
        for future in as_completed(futures):
            tid, table, err = future.result()
            if err == "not_found":
                warn(f"Table not found in BigQuery, skipping: {tid}")
            elif err is not None:
                error(f"Failed to fetch table {tid}", exception=err)
            else:
                bq_tables[tid] = table
    return bq_tables


def _build_update_list(
    bq_tables: Dict[str, Any], nodes_by_table_id: Dict[str, dict]
) -> list:
    """Compare manifest descriptions against BQ and return items that need updating."""
    updates = []
    for table_id, table in bq_tables.items():
        node = nodes_by_table_id[table_id]
        desired_table_desc = _strip_augmented_docs(node.get("description", "") or "")
        manifest_columns = _get_manifest_columns(node)
        current_table_desc = table.description or ""
        table_desc_changed = current_table_desc != desired_table_desc
        column_changes = _find_column_changes(table.schema, manifest_columns)
        if table_desc_changed or column_changes:
            updates.append(
                {
                    "table_id": table_id,
                    "table": table,
                    "desired_table_desc": desired_table_desc,
                    "new_schema": _rebuild_schema_with_descriptions(
                        table.schema, manifest_columns
                    ),
                    "table_desc_changed": table_desc_changed,
                    "column_changes": column_changes,
                }
            )
    return sorted(updates, key=lambda x: x["table_id"])


def _report_and_apply(tables_to_update: list, dry_run: bool, bq_client: Any) -> None:
    """Print a summary of changes and apply them unless dry_run is set."""
    if dry_run:
        info(
            f"Dry-run: {len(tables_to_update)} table(s) have outdated descriptions "
            "(use --apply to update):",
            style="yellow",
        )
    else:
        info(f"Updating descriptions for {len(tables_to_update)} table(s)...")

    for item in tables_to_update:
        if dry_run:
            parts = []
            if item["table_desc_changed"]:
                parts.append("table description")
            if item["column_changes"]:
                cols = ", ".join(col for col, _, _ in item["column_changes"])
                parts.append(f"{len(item['column_changes'])} column(s): {cols}")
            info(f"- {item['table_id']}  ({' | '.join(parts)})", style="bold")
        else:
            info(f"- {item['table_id']}", style="bold")
            _apply_update(bq_client, item)

    if not dry_run:
        info(
            f"Done. Updated descriptions for {len(tables_to_update)} table(s).",
            style="green",
        )


def _apply_update(bq_client: Any, item: dict) -> None:
    """Push description and schema changes for a single table to BigQuery."""
    try:
        table = item["table"]
        table.description = item["desired_table_desc"]
        table.schema = item["new_schema"]
        bq_client.update_table(table, ["description", "schema"])
    except Exception as e:
        error(f"Failed to update {item['table_id']}", exception=e)


def sync_descriptions(
    dry_run: bool = True,
    model_names: Optional[List[str]] = None,
    manifest_path: Path = Manifest.PROD_MANIFEST_PATH,
    impersonate: bool = False,
) -> None:
    """Sync table and column descriptions from a manifest to BigQuery."""
    if manifest_path == Manifest.PROD_MANIFEST_PATH:
        Manifest.download_prod_manifest(force=True)
    manifest = Manifest.get_manifest(manifest_path)

    config = project_config()
    client = BigQueryClient(
        default_project=config.service_account_project,
        impersonation_service_account=config.service_account_identifier
        if impersonate
        else None,
    )
    bq_client = client.get_client()
    _expand_bq_connection_pool(bq_client)

    nodes_by_table_id = _collect_nodes(manifest, model_names)
    if not nodes_by_table_id:
        info("No matching models found.")
        return

    info(f"Fetching {len(nodes_by_table_id)} table(s) from BigQuery in parallel...")
    bq_tables = _fetch_bq_tables(bq_client, nodes_by_table_id, client.NotFound)

    tables_to_update = _build_update_list(bq_tables, nodes_by_table_id)
    if not tables_to_update:
        info("All BigQuery descriptions are already up to date.")
        return

    _report_and_apply(tables_to_update, dry_run, bq_client)
