from dbtwiz.config.project import project_config
from dbtwiz.dbt.manifest import Manifest
from dbtwiz.dbt.project import Profile
from dbtwiz.dbt.target import Target
from dbtwiz.gcp.auth import ensure_auth
from dbtwiz.gcp.bigquery import BigQueryClient
from dbtwiz.helpers.logger import error, info
from dbtwiz.ui.interact import multiselect_from_list


def empty_development_dataset(target_name: str, force_delete: bool) -> None:
    """Delete all materializations in the development dataset"""
    ensure_auth()

    dev_profile = Profile().profile_config(target_name=target_name)
    project = dev_profile.get("project") or dev_profile.get("database")
    dataset = dev_profile.get("dataset") or dev_profile.get("schema")

    client = BigQueryClient(default_project=project_config().user_project)
    tables, _ = client.fetch_tables_in_dataset(project, dataset)
    if not tables:
        info(f"Dataset {project}.{dataset} is already empty.")
        return
    info(
        f"There are {len(list(tables))} tables/views in the {project}.{dataset} dataset."
    )
    if not force_delete:
        answer = input("Delete all tables/views? (y/N)? ")
        if answer.lower() not in ["y", "yes"]:
            return
    for table in tables:
        table_id = f"{project}.{dataset}.{table}"
        try:
            client.delete_table(table_id)
            info(
                f"Deleted {table_id}",
                style="red",
            )
        except Exception as e:
            error(f"Failed to delete {table_id}: {e}")


def build_data_structure(manifest_models, client):
    """
    Build a data structure containing relations from the manifest
    and materializations from BigQuery's information schema.
    """
    # Build structure of all relations appearing in the target's manifest
    data = dict()
    for model in manifest_models:
        project, dataset, table = model["relation_name"].replace("`", "").split(".")
        data[project] = data.get(project, dict())
        data[project][dataset] = data[project].get(dataset, dict(manifest=[]))
        data[project][dataset]["manifest"].append(table)

    # Add existing materializations in DWH by querying information schema
    for project in data.keys():
        try:
            info(f"Fetching datasets and tables for project {project}")
            query = f"""
                select table_schema, array_agg(table_name) as tables
                from {project}.`region-eu`.INFORMATION_SCHEMA.TABLES
                where table_name not like '%__dbt_tmp_%'
                group by table_schema
            """
            result = client.run_query(query).result()
            for row in result:
                dataset = row["table_schema"]
                data[project][dataset] = data[project].get(dataset, dict(manifest=[]))
                data[project][dataset]["bigquery"] = row["tables"]
        except Exception as e:
            error(str(e))

    return data


def find_orphaned_tables(data: dict) -> list:
    """
    Identify orphaned tables in the data structure. A table is considered orphaned
    if it exists in the "bigquery" list but not in the "manifest" list, provided
    that the "manifest" list is not empty.
    """
    orphaned = []
    for project, datasets in data.items():
        for dataset, variants in datasets.items():
            for table in variants.get("bigquery", []):
                if table not in variants["manifest"]:
                    orphaned.append(f"{project}.{dataset}.{table}")
    return orphaned


def handle_orphaned_materializations(
    target: Target, list_only: bool, force_delete: bool
) -> None:
    """List or delete orphaned materializations"""
    ensure_auth()

    Manifest.update_manifests(target, force=True)

    if target == Target.dev:
        manifest = Manifest()
        client = BigQueryClient(default_project=project_config().user_project)
    else:
        manifest = Manifest(Manifest.PROD_MANIFEST_PATH)
        force_delete = False  # Always ask before deleting in prod!
        # Use service account impersonation for prod
        client = BigQueryClient(
            impersonation_service_account=project_config().service_account_identifier,
            default_project=project_config().service_account_project,
        )

    manifest_models = [
        m
        for m in manifest.models().values()
        if m["materialized"] in ["view", "table", "incremental"]
    ]

    # Build structure of all relations appearing in the target's manifest
    data = build_data_structure(manifest_models, client)

    # Build list of orphaned DWH materializations that are no longer in the manifest
    orphaned = find_orphaned_tables(data)

    if len(orphaned) == 0:
        info("There are no orphaned materializations.")
        return

    info(f"Found {len(orphaned)} orphaned materializations.\n", style="yellow")

    if list_only:
        info("Not in manifest:", style="yellow")
        for table_id in sorted(orphaned):
            info(f"- {table_id}", style="yellow")
    else:
        # Prompt user to select tables to delete
        selected_tables = (
            multiselect_from_list(
                "Select orphaned tables to delete",
                items=sorted(orphaned),
                allow_none=True,
            )
            or []
        )

        eligible_projects = [
            "amedia-adp-dbt-core",
            "amedia-adp-marts",
            "amedia-adp-bespoke",
        ]

        for table_id in selected_tables:
            project_name = table_id.split(".")[0]
            if force_delete and not project_name == "amedia-adp-dbt-dev":
                info("Can't force delete unless dev!", style="yellow")
                continue
            elif target == Target.prod and project_name not in eligible_projects:
                info(
                    f"Can't delete table from project {project_name}. Must be one of {', '.join(eligible_projects)}",
                    style="yellow",
                )
                continue
            client.delete_table(table_id=table_id)
            info(f"Deleted {table_id}.")
