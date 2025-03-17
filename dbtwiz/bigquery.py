from typing import List, Tuple

from dbtwiz.logging import info
from ruamel.yaml.scalarstring import PreservedScalarString


class BigQueryClient:
    """Class for BigQuery client"""

    def __init__(self):
        """Initializes the class."""
        from google.api_core.exceptions import Forbidden, NotFound

        self.Forbidden = Forbidden
        self.NotFound = NotFound
        self._bigquery = None
        self._client = None

    def get_bigquery(self):
        """Get or import the BigQuery package."""
        if self._bigquery is None:
            from google.cloud import bigquery

            self._bigquery = bigquery
        return self._bigquery

    def get_client(self):
        """Get or set the BigQuery client."""
        if self._client is None:
            bigquery = self.get_bigquery()
            self._client = bigquery.Client()
        return self._client

    def list_datasets_in_project(self, project) -> Tuple[List[str], str]:
        """Fetch all datasets in the given project from BigQuery."""
        try:
            datasets = list(self.get_client().list_datasets(project=project))
            return sorted([dataset.dataset_id for dataset in datasets]), ""
        except Exception as e:
            return [], f"Error: Failed to fetch datasets from BigQuery: {e}"

    def fetch_tables_in_dataset(self, project, dataset) -> Tuple[List[str], str]:
        """Fetch all tables in the given project and dataset from BigQuery."""
        dataset_ref = f"{project}.{dataset}"
        try:
            tables = list(self.get_client().list_tables(dataset_ref))
            return [table.table_id for table in tables], ""
        except self.NotFound:
            return [], f"Error: The dataset '{dataset_ref}' does not exist in BigQuery."
        except self.Forbidden:
            return [], f"Error: You do not have access to the dataset '{dataset_ref}'."
        except Exception as e:
            return [], f"Error: Failed to fetch tables from BigQuery: {e}"

    def parse_schema(self, fields, prefix=""):
        """
        Parses the schema for a table and returns all the columns.
        If a column is a struct then it recursively adds all nested columns.
        """
        schema_details = []

        for field in fields:
            if field.field_type == "RECORD":
                # Recursively unnest fields within the struct
                nested_fields = self.parse_schema(
                    field.fields, prefix=f"{prefix}{field.name}."
                )
                schema_details.extend(nested_fields)
            else:
                column = {"name": f"{prefix}{field.name}"}
                if field.description:
                    column["description"] = PreservedScalarString(field.description)
                schema_details.append(column)

        return schema_details

    def fetch_table_columns(
        self, project, dataset, table_name
    ) -> Tuple[List[str], str]:
        """Fetch column names and descriptions from BigQuery."""
        table_ref = f"{project}.{dataset}.{table_name}"
        try:
            table = self.get_client().get_table(table_ref)
            columns = self.parse_schema(table.schema)
            return columns, ""
        except self.NotFound:
            return None, f"Error: The table '{table_name}' does not exist in BigQuery."
        except self.Forbidden:
            return None, f"Error: You do not have access to the table '{table_name}'."
        except Exception as e:
            return None, f"Error: Failed to fetch table details from BigQuery: {e}"

    def check_project_exists(self, project) -> str:
        """Checks whether the given project exists in BigQuery"""
        try:
            # Check if the project exists and is accessible
            datasets = list(self.get_client().list_datasets(project=project))
            if not datasets:
                return (
                    f"Warning: The project '{project}' exists but contains no datasets."
                )
            else:
                return "Exists"
        except self.NotFound:
            return f"Error: The project '{project}' does not exist."
        except self.Forbidden:
            return f"Error: You do not have access to the project '{project}'."
        except Exception as e:
            return f"Error: Failed to verify project '{project}': {e}"

    def run_query(self, project, query):
        """Runs a query in bigquery"""
        return self.get_client().query(query, project=project)

    def delete_table(self, table_id, project=None):
        """Deletes a table from bigquery"""
        self.get_client().delete_table(table_id, project=project)

    def get_bigquery_partition_expiration(self, table_id: str) -> int:
        """Get the current partition expiration for a table in BigQuery."""
        table = self.get_client().get_table(table_id)
        if (
            table.time_partitioning
            and table.time_partitioning.expiration_ms is not None
        ):
            return table.time_partitioning.expiration_ms // (
                1000 * 60 * 60 * 24
            )  # Convert ms to days
        return -1  # Return -1 if no expiration is set

    def update_bigquery_partition_expiration(self, table_id: str, expiration_days: int):
        """Update the partition expiration for a table in BigQuery."""
        table = self.get_client().get_table(table_id)
        if table.time_partitioning:
            # Create a new TimePartitioning object with the updated expiration
            updated_partitioning = self.get_bigquery().TimePartitioning(
                type_=table.time_partitioning.type_,
                field=table.time_partitioning.field,
                expiration_ms=expiration_days
                * 24
                * 60
                * 60
                * 1000,  # Convert days to ms
            )
            # Update the table with the new TimePartitioning configuration
            table.time_partitioning = updated_partitioning
            info(
                f"Updating partition expiration for {table_id} to {expiration_days} days"
            )
            self.get_client().update_table(table, ["time_partitioning"])
        else:
            info(f"Table {table_id} is not partitioned. Skipping update.")
