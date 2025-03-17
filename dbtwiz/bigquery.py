from importlib import import_module
from typing import List, Tuple

from ruamel.yaml.scalarstring import PreservedScalarString


def import_module_once(parent, subpackage, submodule=None):
    """Function to enable lazy import and only import once."""
    full_module_name = f"{parent}.{subpackage}" if parent else subpackage
    if full_module_name not in globals():
        globals()[full_module_name] = import_module(full_module_name)
    # If submodule is specified, get it from the imported module
    if submodule:
        return getattr(globals()[full_module_name], submodule)
    return globals()[full_module_name]


def import_common_modules():
    """Import function for common packages."""
    bigquery = import_module_once("google.cloud", "bigquery")
    Forbidden = import_module_once("google.api_core", "exceptions", "Forbidden")
    NotFound = import_module_once("google.api_core", "exceptions", "NotFound")
    return bigquery, Forbidden, NotFound


def list_datasets_in_project(project) -> Tuple[List[str], str]:
    """Fetch all datasets in the given project from BigQuery."""
    bigquery = import_module_once("google.cloud", "bigquery")
    client = bigquery.Client()
    try:
        datasets = list(client.list_datasets(project=project))
        return sorted([dataset.dataset_id for dataset in datasets]), ""
    except Exception as e:
        return [], f"Error: Failed to fetch datasets from BigQuery: {e}"


def fetch_tables_in_dataset(project, dataset) -> Tuple[List[str], str]:
    """Fetch all tables in the given project and dataset from BigQuery."""
    bigquery, Forbidden, NotFound = import_common_modules()
    client = bigquery.Client()
    dataset_ref = f"{project}.{dataset}"
    try:
        tables = list(client.list_tables(dataset_ref))
        return [table.table_id for table in tables], ""
    except NotFound:
        return [], f"Error: The dataset '{dataset_ref}' does not exist in BigQuery."
    except Forbidden:
        return [], f"Error: You do not have access to the dataset '{dataset_ref}'."
    except Exception as e:
        return [], f"Error: Failed to fetch tables from BigQuery: {e}"


def parse_schema(fields, prefix=""):
    """
    Parses the schema for a table and returns all the columns.
    If a column is a struct then it recursively adds all nested columns.
    """
    schema_details = []

    for field in fields:
        if field.field_type == "RECORD":
            # Recursively unnest fields within the struct
            nested_fields = parse_schema(field.fields, prefix=f"{prefix}{field.name}.")
            schema_details.extend(nested_fields)
        else:
            column = {"name": f"{prefix}{field.name}"}
            if field.description:
                column["description"] = PreservedScalarString(field.description)
            schema_details.append(column)

    return schema_details


def fetch_table_columns(project, dataset, table_name) -> Tuple[List[str], str]:
    """Fetch column names and descriptions from BigQuery."""
    bigquery, Forbidden, NotFound = import_common_modules()
    client = bigquery.Client()
    table_ref = f"{project}.{dataset}.{table_name}"
    try:
        table = client.get_table(table_ref)
        columns = parse_schema(table.schema)
        return columns, ""
    except NotFound:
        return None, f"Error: The table '{table_name}' does not exist in BigQuery."
    except Forbidden:
        return None, f"Error: You do not have access to the table '{table_name}'."
    except Exception as e:
        return None, f"Error: Failed to fetch table details from BigQuery: {e}"


def check_project_exists(project) -> str:
    """Checks whether the given project exists in BigQuery"""
    bigquery, Forbidden, NotFound = import_common_modules()
    client = bigquery.Client()
    try:
        # Check if the project exists and is accessible
        datasets = list(client.list_datasets(project=project))
        if not datasets:
            return f"Warning: The project '{project}' exists but contains no datasets."
        else:
            return "Exists"
    except NotFound:
        return f"Error: The project '{project}' does not exist."
    except Forbidden:
        return f"Error: You do not have access to the project '{project}'."
    except Exception as e:
        return f"Error: Failed to verify project '{project}': {e}"


def run_bq_query(project, query):
    """Runs a query in bigquery"""
    bigquery = import_module_once("google.cloud", "bigquery")
    client = bigquery.Client(project=project)
    return client.query(query)


def delete_bq_table(table_id):
    """Deletes a bq table"""
    bigquery = import_module_once("google.cloud", "bigquery")
    client = bigquery.Client()
    client.delete_table(table_id)

# Get the current partition expiration from BigQuery
def get_bigquery_partition_expiration(table_id: str) -> int:
    """Get the current partition expiration for a table in BigQuery."""
    bigquery = import_module_once("google.cloud", "bigquery")
    client = bigquery.Client()
    table = client.get_table(table_id)
    if table.time_partitioning and table.time_partitioning.expiration_ms is not None:
        return table.time_partitioning.expiration_ms // (1000 * 60 * 60 * 24)  # Convert ms to days
    return 0  # Return 0 if no expiration is set

# Update partition expiration in BigQuery
def update_bigquery_partition_expiration(table_id: str, expiration_days: int):
    """Update the partition expiration for a table in BigQuery."""
    bigquery = import_module_once("google.cloud", "bigquery")
    client = bigquery.Client()
    table = client.get_table(table_id)
    if table.time_partitioning:
        table.time_partitioning.expiration_ms = expiration_days * 24 * 60 * 60 * 1000  # Convert days to ms
        client.update_table(table, ["time_partitioning.expiration_ms"])
        print(f"Updated partition expiration for {table_id} to {expiration_days} days")
    else:
        print(f"Table {table_id} is not partitioned. Skipping update.")
