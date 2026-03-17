from ..config.project import project_config
from ..integrations.bigquery import BigQueryClient
from ..integrations.gcp_auth import ensure_auth
from ..utils.logger import fatal, info


def update_clustering(table_id: str, cluster_columns: list[str]) -> None:
    """Update the clustering configuration for a BigQuery table and re-cluster existing rows.

    Args:
        table_id: Full table ID (project.dataset.table)
        cluster_columns: Columns to cluster on (in order)
    """
    ensure_auth()

    client = BigQueryClient(
        impersonation_service_account=project_config().service_account_identifier,
        default_project=project_config().service_account_project,
    )
    bq = client.get_client()

    # Step 1: Update the clustering specification
    info(f"Fetching table metadata for {table_id}...")
    try:
        table = bq.get_table(table_id)
    except client.NotFound:
        fatal(f"Table {table_id} not found.")

    info(f"Updating clustering specification to: {cluster_columns}")
    table.clustering_fields = cluster_columns
    bq.update_table(table, ["clustering_fields"])
    info("Clustering specification updated.")

    # Step 2: Build WHERE clause for the re-clustering UPDATE
    if table.require_partition_filter:
        partition_field = (
            table.time_partitioning.field if table.time_partitioning else None
        )
        where_clause = (
            f"{partition_field} IS NOT NULL"
            if partition_field
            else "_PARTITIONDATE IS NOT NULL"
        )
    else:
        where_clause = "true"

    trigger_col = cluster_columns[0]
    sql = f"UPDATE `{table_id}` SET {trigger_col}={trigger_col} WHERE {where_clause}"

    # Step 3: Run UPDATE to re-cluster all existing rows
    info("Starting re-clustering job...")
    try:
        job = client.run_query(sql)
        job.result()  # Block until complete
        info(f"Re-clustering job succeeded (job id: {job.job_id}).")
    except Exception as e:
        fatal(f"Re-clustering job failed: {e}")
