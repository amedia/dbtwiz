from typing import List, Tuple

from google.api_core.exceptions import Forbidden, NotFound
from google.cloud import bigquery
from ruamel.yaml.scalarstring import PreservedScalarString


def list_datasets_in_project(project) -> Tuple[List[str], str]:
    """Fetch all datasets in the given project from BigQuery."""
    client = bigquery.Client()
    try:
        datasets = list(client.list_datasets(project=project))
        return sorted([dataset.dataset_id for dataset in datasets]), ""
    except Exception as e:
        return [], f"Error: Failed to fetch datasets from BigQuery: {e}"


def fetch_tables_in_dataset(project, dataset) -> Tuple[List[str], str]:
    """Fetch all tables in the given project and dataset from BigQuery."""
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


def fetch_table_columns(project, dataset, table_name) -> Tuple[List[str], str]:
    """Fetch column names and descriptions from BigQuery."""
    client = bigquery.Client()
    table_ref = f"{project}.{dataset}.{table_name}"
    try:
        table = client.get_table(table_ref)
        columns = []
        for field in table.schema:
            column = {"name": field.name}
            if field.description:  # Only add description if it exists
                column["description"] = PreservedScalarString(field.description)
            columns.append(column)
        return columns, ""
    except NotFound:
        return None, f"Error: The table '{table_name}' does not exist in BigQuery."
    except Forbidden:
        return None, f"Error: You do not have access to the table '{table_name}'."
    except Exception as e:
        return None, f"Error: Failed to fetch table details from BigQuery: {e}"


def check_project_exists(project) -> str:
    """Checks whether the given project exists in BigQuery"""
    client = bigquery.Client()
    try:
        # Check if the project exists and is accessible
        datasets = list(client.list_datasets(project=project))
        if not datasets:
            return f"Warning: The database '{project}' exists but contains no datasets."
        else:
            return "Exists"
    except NotFound:
        return f"Error: The database '{project}' does not exist."
    except Forbidden:
        return f"Error: You do not have access to the database '{project}'."
    except Exception as e:
        return f"Error: Failed to verify project '{project}': {e}"

def run_bq_query(project, query):
    """Runs a query in bigquery"""
    client = bigquery.Client(project=project)
    return client.query(query)

def delete_bq_table(table_id):
    """Deletes a bq table"""
    client = bigquery.Client()
    client.delete_table(table_id)
