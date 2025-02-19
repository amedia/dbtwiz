from dbtwiz.auth import ensure_auth
from dbtwiz.manifest import Manifest
from dbtwiz.target import Target
from dbtwiz.logging import info

from pathlib import Path
from google.cloud import bigquery


def cleanup_materializations(target: Target, list_only: bool, force_delete: bool) -> None:
    """Delete orphaned materializations"""

    ensure_auth()

    Manifest.update_manifests(target)

    if target == Target.dev:
        manifest = Manifest()
    else:
        manifest = Manifest(Manifest.PROD_MANIFEST_PATH)
        force_delete = False  # Always ask before deleting in prod!

    manifest_models = [
        m for m in manifest.models().values()
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

    # Add existing materializations in DWH by querying information schema
    for project, datasets in data.items():
        bq_client = bigquery.Client(project=project)
        info(f"Fetching datasets and tables for project {project}")
        result = bq_client.query(f"""
            select table_schema, array_agg(table_name) as tables
            from region-eu.INFORMATION_SCHEMA.TABLES
            where table_catalog = '{project}'
                and table_name not like '%__dbt_tmp_%'
            group by table_schema
        """).result()
        for row in result:
            dataset = row["table_schema"]
            data[project][dataset] = data[project].get(dataset, dict(manifest=[]))
            data[project][dataset]["bigquery"] = row["tables"]

    # Build list of orphaned DWH materializations that are no longer in the manifest
    orphaned = []
    for project, datasets in data.items():
        for dataset, variants in datasets.items():
            for table in variants["bigquery"]:
                if table not in variants["manifest"] and len(variants["manifest"]) > 0:
                    orphaned.append(f"{project}.{dataset}.{table}")

    if len(orphaned) == 0:
        info("All materializations are in the manifest.")
        return

    info(f"Found {len(orphaned)} materializations not included in the manifest.")
    info("")
    bq_client = bigquery.Client()

    for table_id in sorted(orphaned):
        info(f"Not in manifest: {table_id}")
        if list_only:
            delete = False
        elif force_delete:
            assert table_id.startswith("amedia-adp-dbt-dev."), "Can't force delete unless dev!"
            delete = True
        else:
            answer = input("Delete (y/N)? ")
            delete = answer.lower() in ["y", "yes"]
        if delete:
            bq_client.delete_table(table_id)
            info(f"Deleted {table_id}.")
