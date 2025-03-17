from dbtwiz.auth import ensure_auth
from dbtwiz.bigquery import BigQueryClient
from dbtwiz.logging import error, info
from dbtwiz.manifest import Manifest
from dbtwiz.target import Target


def empty_development_dataset(force_delete: bool) -> None:
    """Delete all materializations in the development dataset"""
    ensure_auth()

    Manifest.update_manifests("dev")
    manifest = Manifest()

    # Get project and dataset from first materialization in dev manifest
    project, dataset = [
        (m["database"], m["schema"])
        for m in manifest.models().values()
        if m["materialized"] != "ephemeral"
    ][0]

    client = BigQueryClient()
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
        table_type = table.table_type.lower()
        try:
            client.delete_bq_table(table, project)
            info(f"Deleted {table_type} {project}.{dataset}.{table.table_id}")
        except Exception as e:
            error(
                f"Failed to delete {table_type} {project}.{dataset}.{table.table_id}: {e}"
            )


def handle_orphaned_materializations(
    target: Target, list_only: bool, force_delete: bool
) -> None:
    """List or delete orphaned materializations"""
    ensure_auth()

    Manifest.update_manifests(target)

    if target == Target.dev:
        manifest = Manifest()
    else:
        manifest = Manifest(Manifest.PROD_MANIFEST_PATH)
        force_delete = False  # Always ask before deleting in prod!

    manifest_models = [
        m
        for m in manifest.models().values()
        if m["materialized"] in ["view", "table", "incremental"]
    ]

    # Build structure of all relations appearing in the target's manifest
    data = dict()
    for model in manifest_models:
        project, dataset, table = model["relation_name"].replace("`", "").split(".")
        if dataset == "elementary":  # Skip materializations belonging to Elementary
            continue
        data[project] = data.get(project, dict())
        data[project][dataset] = data[project].get(dataset, dict(manifest=[]))
        data[project][dataset]["manifest"].append(table)

    client = BigQueryClient()

    # Add existing materializations in DWH by querying information schema
    for project, datasets in data.items():
        info(f"Fetching datasets and tables for project {project}")
        result = client.run_bq_query(
            project,
            f"""
            select table_schema, array_agg(table_name) as tables
            from region-eu.INFORMATION_SCHEMA.TABLES
            where table_catalog = '{project}'
                and table_name not like '%__dbt_tmp_%'
            group by table_schema
        """,
        ).result()
        for row in result:
            dataset = row["table_schema"]
            data[project][dataset] = data[project].get(dataset, dict(manifest=[]))
            data[project][dataset]["bigquery"] = row["tables"]

    # Build list of orphaned DWH materializations that are no longer in the manifest
    orphaned = []
    for project, datasets in data.items():
        for dataset, variants in datasets.items():
            for table in variants.get("bigquery", []):
                if table not in variants["manifest"] and len(variants["manifest"]) > 0:
                    orphaned.append(f"{project}.{dataset}.{table}")

    if len(orphaned) == 0:
        info("There are no orphaned materializations.")
        return

    info(f"Found {len(orphaned)} orphaned materializations.")
    info("")

    for table_id in sorted(orphaned):
        info(f"Not in manifest: {table_id}")
        if list_only:
            delete = False
        elif force_delete:
            assert table_id.startswith("amedia-adp-dbt-dev."), (
                "Can't force delete unless dev!"
            )
            delete = True
        else:
            answer = input("Delete (y/N)? ")
            delete = answer.lower() in ["y", "yes"]
        if delete:
            client.delete_bq_table(table_id)
            info(f"Deleted {table_id}.")
