from typing import Dict, List, Tuple

from ruamel.yaml.scalarstring import PreservedScalarString

from dbtwiz.logging import info, error

MILLISECONDS_PER_DAY = 1000 * 60 * 60 * 24


class BigQueryClient:
    """Class for BigQuery client"""

    def __init__(
        self, impersonation_service_account: str = None, default_project: str = None
    ):
        """Initializes the class."""
        from google.api_core.exceptions import Forbidden, NotFound

        self.Forbidden = Forbidden
        self.NotFound = NotFound
        self.impersonation_service_account = impersonation_service_account
        self.default_project = default_project
        self._bigquery = None
        self._client = None
        self._credentials = None
        self._authorized_session = None

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
            credentials = self.get_credentials()
            self._client = bigquery.Client(
                credentials=credentials, project=self.default_project
            )
        return self._client

    def get_credentials(self):
        """Retrieve or create credentials, optionally with impersonation."""
        if self._credentials is None:
            from google.auth import default
            from google.auth.impersonated_credentials import Credentials

            credentials, project = default()

            # Use the project from default credentials if no default_project is provided
            if not self.default_project:
                self.default_project = project

            # Impersonate the target service account if specified
            if self.impersonation_service_account:
                self._credentials = Credentials(
                    source_credentials=credentials,
                    target_principal=self.impersonation_service_account,
                    target_scopes=["https://www.googleapis.com/auth/cloud-platform"],
                )
            else:
                self._credentials = credentials

        return self._credentials

    def get_authorized_session(self):
        """Retrieve or create authorized session."""
        if self._authorized_session is None:
            from google.auth.transport.requests import AuthorizedSession

            self._authorized_session = AuthorizedSession(self.get_credentials())

        return self._authorized_session

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

    def delete_table(self, table_id):
        """Deletes a table from bigquery"""
        self.get_client().delete_table(table_id)

    def get_bigquery_partition_expiration(self, table_id: str) -> int:
        """Get the current partition expiration for a table in BigQuery."""
        table = self.get_client().get_table(table_id)
        if (
            table.time_partitioning
            and table.time_partitioning.expiration_ms is not None
        ):
            return (
                table.time_partitioning.expiration_ms // MILLISECONDS_PER_DAY
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
                * MILLISECONDS_PER_DAY,  # Convert days to ms
            )
            # Update the table with the new TimePartitioning configuration
            table.time_partitioning = updated_partitioning
            info(
                f"Updating partition expiration for {table_id} to {expiration_days} days"
            )
            self.get_client().update_table(table, ["time_partitioning"])
        else:
            info(f"Table {table_id} is not partitioned. Skipping update.")

    def update_table_constraints(self, table_id: str, table_constraints: Dict) -> None:
        """
        Updates the table constraints (primary/foreign keys) using the BigQuery REST API.

        Args:
            table_id: The full table ID (e.g., 'project.dataset.table').
            table_constraints: The table constraints to apply.
        """
        try:
            # Construct the REST API URL
            project_id, dataset_id, table_id_only = table_id.split(".")
            table_path = f"https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}/datasets/{dataset_id}/tables/{table_id_only}"

            # Prepare the request body
            body = {
                "tableReference": {
                    "projectId": project_id,
                    "datasetId": dataset_id,
                    "tableId": table_id_only,
                },
            }

            # If table_constraints is None, remove constraints
            if table_constraints is None:
                body["tableConstraints"] = table_constraints
            else:
                # Convert table_constraints to a dictionary
                new_table_constraints = {}
                if table_constraints.primary_key:
                    new_table_constraints["primaryKey"] = {
                        "columns": table_constraints.primary_key.columns,
                    }
                if table_constraints.foreign_keys:
                    new_table_constraints["foreignKeys"] = [
                        {
                            "name": fk.name,
                            "referencedTable": {
                                "projectId": fk.referenced_table.project_id,
                                "datasetId": fk.referenced_table.dataset_id,
                                "tableId": fk.referenced_table.table_id,
                            },
                            "columnReferences": [
                                {
                                    "referencingColumn": ref.referencing_column,
                                    "referencedColumn": ref.referenced_column,
                                }
                                for ref in fk.column_references
                            ],
                        }
                        for fk in table_constraints.foreign_keys
                    ]
                body["tableConstraints"] = new_table_constraints

            # Create an authorized session
            authed_session = self.get_authorized_session()
            # Send the PATCH request to update the table constraints
            response = authed_session.patch(table_path, json=body)
            response.raise_for_status()

        except Exception as e:
            error(f"Error updating table constraints for {table_id}: {e}")

    def copy_properties(self, source_table, destination_table, property_type) -> None:
        """
        Copies all relevant properties from a source table to a destination table.
        """
        # Copy universal properties
        destination_table.description = source_table.description
        destination_table.labels = source_table.labels
        if hasattr(source_table, "default_collation"):
            destination_table.default_collation = source_table.default_collation

        # Copy table specific options
        if property_type == "TABLE":
            # Copy schema (includes column descriptions)
            destination_table.schema = source_table.schema

            # Copy general properties
            destination_table.time_partitioning = source_table.time_partitioning
            destination_table.range_partitioning = source_table.range_partitioning
            destination_table.clustering_fields = source_table.clustering_fields
            destination_table.expires = source_table.expires
            destination_table.encryption_configuration = (
                source_table.encryption_configuration
            )
            destination_table.friendly_name = source_table.friendly_name

            # Copy table options
            destination_table.require_partition_filter = (
                source_table.require_partition_filter
            )
            destination_table.max_staleness = source_table.max_staleness

            if hasattr(source_table, "external_data_configuration"):
                destination_table.external_data_configuration = (
                    source_table.external_data_configuration
                )
            if hasattr(source_table, "materialized_view"):
                destination_table.materialized_view = source_table.materialized_view

        # Copy view specific options
        elif property_type == "VIEW":
            if hasattr(source_table, "view_options"):
                destination_table.view_options = source_table.view_options

    def copy_table(self, old_table_id: str, new_table_id: str) -> None:
        """
        Copies a BigQuery table or view from the old location to the new location.

        Args:
            old_table_id: The full table ID of the source table/view (e.g., 'project_a.dataset_old.table_old').
            new_table_id: The full table ID of the destination table/view (e.g., 'project_b.dataset_new.table_new').
        """
        try:
            client = self.get_client()
            bigquery = self.get_bigquery()
            # Get the source table/view
            source_table = client.get_table(old_table_id)

            # Fetch the IAM policy before renaming/deleting the old table/view
            source_iam_policy = client.get_iam_policy(old_table_id)

            # Check if the source object is a table or a view
            if source_table.table_type == "TABLE":
                # Create a new table with the same definition as the source table
                new_table = bigquery.Table(new_table_id)

                # Copy all table properties
                self.copy_properties(
                    source_table=source_table,
                    destination_table=new_table,
                    property_type="TABLE",
                )

                # Create the new table in BigQuery
                new_table = client.create_table(new_table)
                if source_table.table_constraints:
                    self.update_table_constraints(
                        table_id=new_table_id,
                        table_constraints=source_table.table_constraints,
                    )
                # Copy data from the old table to the new table
                job_config = bigquery.CopyJobConfig()
                job = client.copy_table(source_table, new_table, job_config=job_config)
                job.result()  # Wait for the job to complete
                info(f"Created new table: {new_table_id}")

            elif source_table.table_type == "VIEW":
                # Create a new view with the same definition as the source view
                new_view = bigquery.Table(new_table_id)
                new_view.view_query = source_table.view_query
                self.copy_properties(
                    source_table=source_table,
                    destination_table=new_view,
                    property_type="VIEW",
                )
                new_view = client.create_table(new_view)
                new_view.schema = source_table.schema
                new_view = client.update_table(new_view, ["schema"])
                info(f"Created new view {new_table_id}")

            else:
                raise ValueError(f"Unsupported table type: {source_table.table_type}")

            # Replicate grants from the old table/view to the new table/view
            client.set_iam_policy(new_table_id, source_iam_policy)

        except self.NotFound:
            error(f"Error: Table/view {old_table_id} not found.")
        except Exception as e:
            error(f"Error copying table/view {old_table_id} to {new_table_id}: {e}")

    def migrate_table(
        self,
        old_table_id: str,
        new_table_id: str,
        backup_table_id: str,
    ) -> None:
        """
        Replaces an exisiting table/view with a view to the given new table/view, which must already exist.
        Before doing so, it creates a backup table in the original dataset.

        Args:
            old_table_id: The full table ID of the original table/view (e.g., 'project_a.dataset_old.table_old').
            new_table_id: The full table ID of the new table/view (e.g., 'project_b.dataset_new.table_new').
            backup_table_id: The full table ID of the backup table/view (e.g., 'project_a.dataset_old.table_old__bck').
        """
        try:
            client = self.get_client()
            bigquery = self.get_bigquery()

            # Get the old table/view metadata
            old_table = client.get_table(old_table_id)

            old_table_name = old_table_id.split(".")[-1]
            bck_table_name = backup_table_id.split(".")[-1]

            # Fetch the IAM policy before renaming/deleting the old table/view
            source_iam_policy = client.get_iam_policy(old_table_id)

            # Handle tables and views differently
            if old_table.table_type == "TABLE":
                if old_table.table_constraints:
                    # Remove constraints before renaming
                    self.update_table_constraints(
                        table_id=old_table_id,
                        table_constraints=None,
                    )

                # For tables, use ALTER TABLE ... RENAME TO
                query = f"""
                alter table `{old_table_id}`
                rename to `{bck_table_name}`;
                """
                query_job = client.query(query)
                query_job.result()  # Wait for the job to complete
                bck_table = client.get_table(backup_table_id)
                bck_table.description = (
                    f"THIS TABLE IS FOR BACKUP PURPOSES ONLY. USE {new_table_id}."
                )
                client.update_table(bck_table, ["description"])
                if old_table.table_constraints:
                    # Reapply constraints after renaming
                    self.update_table_constraints(
                        table_id=backup_table_id,
                        table_constraints=old_table.table_constraints,
                    )
                info(f"Renamed table {old_table_id} to {bck_table_name}")

            elif old_table.table_type == "VIEW":
                # For views, create a new view with the same definition
                bck_view = bigquery.Table(backup_table_id)
                bck_view.view_query = old_table.view_query
                self.copy_properties(
                    source_table=old_table,
                    destination_table=bck_view,
                    property_type="VIEW",
                )
                bck_view.description = (
                    f"THIS VIEW IS FOR BACKUP PURPOSES ONLY. USE {new_table_id}."
                )
                bck_view = client.create_table(bck_view)
                bck_view.schema = old_table.schema
                bck_view = client.update_table(bck_view, ["schema"])
                info(f"Created new view {backup_table_id} as copy of {old_table_id}")

                # Delete the original view
                client.delete_table(old_table_id)

            else:
                raise ValueError(f"Unsupported table type: {old_table.table_type}")

            # Create a view at the original table/view location
            view_id = (
                old_table_id  # View will have the same ID as the original table/view
            )
            view = bigquery.Table(view_id)
            view.view_query = f"select * from `{new_table_id}`"
            self.copy_properties(
                source_table=old_table, destination_table=view, property_type="VIEW"
            )
            view.description = f"THIS VIEW IS DEPRECATED. USE {new_table_id}."
            view = client.create_table(view)
            view.schema = old_table.schema
            view = client.update_table(view, ["schema"])
            if old_table.table_constraints:
                # Remove constraints before renaming
                self.update_table_constraints(
                    table_id=view_id,
                    table_constraints=old_table.table_constraints,
                )
            info(f"Created view {view_id}")

            # Replicate grants from the old table/view to the view
            client.set_iam_policy(view_id, source_iam_policy)

        except self.NotFound:
            error(f"Error: Table/view {old_table_id} not found.")
        except Exception as e:
            error(f"Error renaming table/view or creating view: {e}")
            # Roll back changes if any step fails
            if old_table.table_type == "TABLE" and "bck_table_name" in locals():
                # For tables, revert the rename operation
                revert_query = f"""
                alter table `{backup_table_id}`
                rename to `{old_table_name}`;
                """
                try:
                    revert_job = client.query(revert_query)
                    revert_job.result()
                    info(f"Rollback: Renamed {backup_table_id} back to {old_table_id}")
                except Exception as revert_error:
                    error(f"Failed to roll back rename operation: {revert_error}")
            elif old_table.table_type == "VIEW" and "bck_table_name" in locals():
                # For views, delete the new view if it was created
                if client.get_table(bck_table_name, retry=None):
                    client.delete_table(bck_table_name)
                    info(f"Rollback: Deleted {bck_table_name}")
            if "view_id" in locals() and client.get_table(view_id, retry=None):
                client.delete_table(view_id)
                info(f"Rollback: Deleted {view_id}")
