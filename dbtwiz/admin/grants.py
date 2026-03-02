import json
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from ..config.project import project_config
from ..core.project import Project
from ..integrations.bigquery import BigQueryClient
from ..integrations.gcp_auth import ensure_auth
from ..utils.logger import error, info


def _collect_principals(
    meta_values: Any, access_dict: dict, config_key: str, node_name: str
) -> list[str]:
    """Resolve meta config values to principals via access dict lookup."""
    principals = []
    if not meta_values or not access_dict:
        return principals

    if isinstance(meta_values, str):
        meta_values = [meta_values]

    for item in meta_values:
        entry = access_dict.get(item)
        if entry is None:
            valid = "|".join(access_dict.keys())
            raise ValueError(
                f"{node_name} - Unable to find {config_key} with name '{item}'. "
                f"Valid values are: {valid}"
            )
        principal = entry.get("principal")
        if principal:
            if isinstance(principal, list):
                principals.extend(principal)
            else:
                principals.append(principal)

    return principals


def _resolve_desired_grants(
    node: dict,
    teams: dict,
    access_policies: dict,
    service_consumers: dict,
    role: str,
    open_access_group: str,
) -> dict[str, list[str]] | None:
    """Resolve the desired grant config for a node.

    Returns None if grants should be skipped entirely.
    Returns {} if no grants are configured.
    Returns {role: [grantees]} with resolved grants.
    """
    config = node.get("config", {})
    meta = config.get("meta") or {}
    name = node.get("name", "")
    resource_type = node.get("resource_type", "")

    if meta.get("skip_grants", False):
        return None
    if resource_type == "function":
        return None
    if config.get("catalog_name"):
        return None
    if config.get("materialized") == "ephemeral":
        return None

    grantees: set[str] = set()

    explicit_grants = config.get("grants") or {}
    explicit_viewers = explicit_grants.get(role, [])
    if isinstance(explicit_viewers, str):
        explicit_viewers = [explicit_viewers]
    grantees.update(explicit_viewers)

    grantees.update(_collect_principals(meta.get("teams"), teams, "teams", name))

    grantees.update(
        _collect_principals(
            meta.get("access-policy"), access_policies, "access-policy", name
        )
    )

    grantees.update(
        _collect_principals(
            meta.get("service-consumers"), service_consumers, "service-consumers", name
        )
    )

    if open_access_group and config.get("access") in ("protected", "public"):
        grantees.add(open_access_group)

    if not grantees:
        return {}

    invalid = [g for g in grantees if "group:" not in g and "serviceAccount:" not in g]
    if invalid:
        raise ValueError(
            f"Invalid principals '{', '.join(invalid)}'; "
            "Can only grant permissions to groups and service accounts!"
        )

    return {role: sorted(grantees)}


def _resolve_all_grants(
    manifest: dict,
    teams: dict,
    access_policies: dict,
    service_consumers: dict,
    role: str,
    open_access_group: str,
    skip_schemas: set[str],
) -> dict[tuple[str, str], dict[str, dict]]:
    """Resolve desired grants for all nodes in the manifest.

    Returns:
        desired_by_dataset: {(project, dataset): {table_name: {role: [grantees]}}}
    """
    desired_by_dataset: dict[tuple[str, str], dict[str, dict]] = defaultdict(dict)
    errors: list[str] = []
    skipped = 0

    for node in manifest.get("nodes", {}).values():
        resource_type = node.get("resource_type", "")

        if resource_type not in ("model", "seed", "snapshot"):
            continue
        if node.get("package_name") == "elementary":
            continue

        schema = node.get("schema", "")
        name = node.get("name", "")

        try:
            grants = _resolve_desired_grants(
                node, teams, access_policies, service_consumers, role, open_access_group
            )
        except ValueError as e:
            errors.append(str(e))
            continue

        if grants is None:
            skipped += 1
            continue

        if (
            not grants
            and not schema.endswith("_elementary")
            and schema not in skip_schemas
        ):
            errors.append(
                f"No grant config: Model '{schema}.{name}' has no grant config, "
                "and will be unavailable for all as a result."
            )
            continue

        if not grants:
            skipped += 1
            continue

        proj = node.get("database", "")
        alias = node.get("alias", name)
        desired_by_dataset[(proj, schema)][alias] = grants

    if errors:
        info(f"Grant validation warnings ({len(errors)}):", style="yellow")
        for err in errors:
            info(f"  - {err}", style="yellow")

    total_models = sum(len(tables) for tables in desired_by_dataset.values())
    info(
        f"Resolved grants for {total_models} models "
        f"across {len(desired_by_dataset)} datasets (skipped {skipped})"
    )
    return desired_by_dataset


def _expand_bq_connection_pool(native_client: Any) -> None:
    """Patch the BigQuery client's HTTP adapter to allow parallel connections."""
    from requests.adapters import HTTPAdapter

    adapter = HTTPAdapter(pool_connections=128, pool_maxsize=128, max_retries=3)
    native_client._http.mount("https://", adapter)
    native_client._http._auth_request.session.mount("https://", adapter)


def _fetch_table_policy(
    native_client: Any,
    project: str,
    dataset: str,
    table_name: str,
) -> tuple[str, str, str, Any]:
    """Fetch the IAM policy for a single table."""
    policy = native_client.get_iam_policy(f"{project}.{dataset}.{table_name}")
    return project, dataset, table_name, policy


def _fetch_all_current_policies(
    native_client: Any,
    desired_by_dataset: dict[tuple[str, str], dict[str, dict]],
) -> dict[tuple[str, str], dict[str, Any]]:
    """Fetch IAM policies for all tables across all datasets in parallel.

    Returns {(project, dataset): {table_name: policy_object}}
    """
    results: dict[tuple[str, str], dict[str, Any]] = defaultdict(dict)

    with ThreadPoolExecutor(max_workers=32) as executor:
        futures = {
            executor.submit(
                _fetch_table_policy,
                native_client,
                proj,
                dataset,
                table_name,
            ): (proj, dataset, table_name)
            for (proj, dataset), desired_tables in desired_by_dataset.items()
            for table_name in desired_tables
        }

        for future in as_completed(futures):
            try:
                proj, dataset, table_name, policy = future.result()
                results[(proj, dataset)][table_name] = policy
            except Exception as e:
                proj, dataset, table_name = futures[future]
                error(
                    f"Could not fetch IAM policy for {proj}.{dataset}.{table_name}",
                    exception=e,
                )

    return results


def _print_resolved_grants(
    desired_by_dataset: dict[tuple[str, str], dict[str, dict]], role: str
) -> None:
    """Print desired grant counts per model for --resolve-only mode."""
    for (proj, dataset), desired_tables in sorted(desired_by_dataset.items()):
        info(f"\n-- {proj}.{dataset} ({len(desired_tables)} models)", style="white")
        for table_name, grants in sorted(desired_tables.items()):
            info(f"  {table_name}: {len(grants.get(role, []))} grantees", style="white")


def _collect_pending_changes(
    desired_by_dataset: dict[tuple[str, str], dict[str, dict]],
    current_policies: dict[tuple[str, str], dict[str, Any]],
    role: str,
) -> list[tuple]:
    """Compute IAM diffs for all tables.

    Returns a sorted list of (proj, dataset, table_name, policy, desired_members, granting, revoking).
    """
    pending = []
    for (proj, dataset), desired_tables in sorted(desired_by_dataset.items()):
        dataset_policies = current_policies.get((proj, dataset), {})
        for table_name, desired in sorted(desired_tables.items()):
            policy = dataset_policies.get(table_name)
            if policy is None:
                continue
            desired_members = set(desired.get(role, []))
            current_members = set(policy.get(role) or [])
            needs_granting = desired_members - current_members
            needs_revoking = current_members - desired_members
            if needs_granting or needs_revoking:
                pending.append(
                    (
                        proj,
                        dataset,
                        table_name,
                        policy,
                        desired_members,
                        needs_granting,
                        needs_revoking,
                    )
                )
    return pending


def _set_table_policy(
    native_client: Any,
    proj: str,
    dataset: str,
    table_name: str,
    policy: Any,
    role: str,
    desired_members: set[str],
) -> None:
    """Apply a single IAM policy update."""
    policy[role] = sorted(desired_members)
    native_client.set_iam_policy(f"{proj}.{dataset}.{table_name}", policy)


def _apply_grants_changes(
    native_client: Any,
    desired_by_dataset: dict[tuple[str, str], dict[str, dict]],
    current_policies: dict[tuple[str, str], dict[str, Any]],
    role: str,
    dry_run: bool,
) -> tuple[int, int]:
    """Diff, log, and apply IAM policy changes for all tables. Returns (grants, revokes)."""
    pending = _collect_pending_changes(desired_by_dataset, current_policies, role)

    total_grants = 0
    total_revokes = 0
    for proj, dataset, table_name, _, _, needs_granting, needs_revoking in pending:
        parts = []
        if needs_granting:
            g = len(needs_granting)
            parts.append(f"{g} grant{'s' if g > 1 else ''}")
        if needs_revoking:
            r = len(needs_revoking)
            parts.append(f"{r} revoke{'s' if r > 1 else ''}")
        info(f"- {dataset}.{table_name} ({', '.join(parts)})", style="white")
        total_grants += len(needs_granting)
        total_revokes += len(needs_revoking)

    if dry_run or not pending:
        return total_grants, total_revokes

    with ThreadPoolExecutor(max_workers=32) as executor:
        futures = {
            executor.submit(
                _set_table_policy,
                native_client,
                proj,
                dataset,
                table_name,
                policy,
                role,
                desired_members,
            ): (proj, dataset, table_name)
            for proj, dataset, table_name, policy, desired_members, _, _ in pending
        }
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                proj, dataset, table_name = futures[future]
                error(
                    f"Could not update IAM policy for {proj}.{dataset}.{table_name}",
                    exception=e,
                )

    return total_grants, total_revokes


def update_grants(
    manifest_path: Path,
    dry_run: bool,
    resolve_only: bool,
    impersonate: bool,
) -> None:
    """Update BigQuery grants for all dbt models based on manifest configuration."""
    ensure_auth()

    config = project_config()
    role = config.grants_role or "roles/bigquery.dataViewer"
    open_access_group = config.grants_open_access_group or ""
    skip_schemas = set(config.grants_skip_schemas or [])
    impersonation_sa = config.service_account_identifier if impersonate else None

    with open(manifest_path) as f:
        manifest = json.load(f)

    vars_ = Project().data.get("vars", {}) or {}
    teams = dict(vars_.get("teams", {}) or {})
    access_policies = dict(vars_.get("access-policies", {}) or {})
    service_consumers = dict(vars_.get("service-consumers", {}) or {})

    desired_by_dataset = _resolve_all_grants(
        manifest,
        teams,
        access_policies,
        service_consumers,
        role,
        open_access_group,
        skip_schemas,
    )

    if resolve_only:
        _print_resolved_grants(desired_by_dataset, role)
        return

    client = BigQueryClient(
        default_project=config.service_account_project,
        impersonation_service_account=impersonation_sa or None,
    )
    native_client = client.get_client()
    _expand_bq_connection_pool(native_client)

    current_policies = _fetch_all_current_policies(native_client, desired_by_dataset)
    total_grants, total_revokes = _apply_grants_changes(
        native_client, desired_by_dataset, current_policies, role, dry_run
    )

    action = "Would apply" if dry_run else "Applied"
    info(f"{action} {total_grants} grants and {total_revokes} revokes")
