from .manifest import Manifest
from .target import Target
from .logging import info

from pathlib import Path
from google.cloud import bigquery


def cleanup_materializations(target: Target):
    """Delete obsolete materializations"""

    if False: # target != Target.dev:
        Manifest.update_manifests()

    if target == Target.dev:
        manifest = Manifest()
    else:
        manifest = Manifest(Manifest.PROD_MANIFEST_PATH)

    manifest_models = [
        m for m in manifest.models().values()
        if m["materialized"] in ["view", "table", "incremental"]
    ]

    data = dict()
    for model in manifest_models:
        project, dataset, table = model["database"], model["schema"], model["name"]
        # manifest_datasets[database] = manifest_datasets.get(database, set()).union({model["schema"]})
        data[project] = data.get(project, dict())
        data[project][dataset] = data[project].get(dataset, dict(manifest=[]))
        data[project][dataset]["manifest"].append(table)

    for project, datasets in data.items():
        bq = bigquery.Client(project=project)
        # for dataset in datasets.keys():
        #     print(f"Looking up tables in {project=}, {dataset=}")
        #     data[project][dataset]["bigquery"] = [
        #         t.table_id for t in bq.list_tables(f"{project}.{dataset}")
        #         if not "__dbt_tmp_" in t.table_id]
        print(f"Fetching datasets and tables for project {project}")
        result = bq.query(f"""
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

    for project, datasets in data.items():
        for dataset, variants in datasets.items():
            for table in variants["bigquery"]:
                if table not in variants["manifest"] and len(variants["manifest"]) > 0:
                    print(f"Not in manifest: {project}.{dataset}.{table}")
