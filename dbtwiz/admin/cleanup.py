import os
import subprocess
from datetime import datetime

from ..config.project import project_config
from ..core.project import Profile
from ..dbt.manifest import Manifest
from ..dbt.target import Target
from ..integrations.bigquery import BigQueryClient
from ..integrations.gcp_auth import ensure_auth
from ..ui.interact import confirm, multiselect_from_list
from ..utils.logger import error, info


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
        if not confirm("Delete all tables/views?"):
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
    # Exclude any table that is set to expire within the next 24 hours
    for project in data.keys():
        try:
            info(f"Fetching datasets and tables for project {project}")
            query = f"""
                select t.table_schema, array_agg(t.table_name) as tables
                from {project}.`{project_config().orphan_cleanup_bq_region}`.INFORMATION_SCHEMA.TABLES as t
                left join {project}.`{project_config().orphan_cleanup_bq_region}`.INFORMATION_SCHEMA.TABLE_OPTIONS as o
                    on t.table_name = o.table_name AND o.option_name = 'expiration_timestamp'
                where
                    t.table_name not like '%__dbt_tmp_%'
                    and (
                        o.option_value is null
                        or cast(replace(replace(option_value, 'TIMESTAMP ', ''), '"', '') as timestamp)
                        > timestamp_add(current_timestamp(), interval 24 hour)
                    )
                group by t.table_schema
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


def parse_git_log_output(models_path):
    """
    Parses the git log output in order to fetch information about when and by whom
    a model was deleted.
    """
    try:
        # Ensure full history is available
        subprocess.run(["git", "fetch", "--unshallow"], stderr=subprocess.DEVNULL)

        git_output = subprocess.check_output(
            [
                "git",
                "log",
                "--diff-filter=D",
                "--summary",
                "--pretty=format:commit %H%nAuthor: %an <%ae>%nDate: %ad%n%n%s%n",
                models_path,
            ],
            stderr=subprocess.DEVNULL,
            text=True,
        )
    except subprocess.CalledProcessError:
        return []

    lines = git_output.splitlines()
    deleted_files = []
    current_commit = current_date = current_author = current_message = None

    for line in lines:
        if line.startswith("commit "):
            current_commit = line.split()[1]
            current_author = current_date = current_message = None
        elif line.startswith("Author:"):
            raw_author = line.replace("Author:", "").strip()
            current_author = raw_author.split(" <")[0]
        elif line.startswith("Date:"):
            current_date = line.replace("Date:", "").strip()
        elif current_message is None and line.strip():
            current_message = line.strip()
        elif "delete mode" in line:
            deleted_file = line.split()[-1]
            deleted_files.append(
                {
                    "file": deleted_file,
                    "commit": current_commit,
                    "timestamp": current_date,
                    "author": current_author,
                    "message": current_message,
                }
            )

    return deleted_files


def match_table_to_deletion(table_name, deleted_files):
    """
    Matches a table name to a delete file by either looking at a perfect match
    or by a match for what follows "__".
    """
    for entry in deleted_files:
        filename = os.path.basename(entry["file"])
        name, _ = os.path.splitext(filename)
        if name == table_name or name.endswith(f"__{table_name}"):
            return entry
    return None


def format_deletion_timestamp(deletion_timestamp):
    """
    Fomats a git deletion timestamp to our desired format.
    """
    try:
        dt = datetime.strptime(deletion_timestamp, "%a %b %d %H:%M:%S %Y %z")
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return deletion_timestamp


def get_github_commit_url(commit_id):
    """
    Gets the github commit url by identifying github owner and repo, and combining
    it with a commit id into a full url.
    """
    try:
        # Get the remote URL
        remote_url = subprocess.check_output(
            ["git", "config", "--get", "remote.origin.url"],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()

        # Handle both HTTPS and SSH formats
        if remote_url.startswith("git@github.com:"):
            path = remote_url.split("git@github.com:")[1]
        elif remote_url.startswith("https://github.com/"):
            path = remote_url.split("https://github.com/")[1]
        else:
            return None, None

        # Remove .git suffix if present
        path = path.replace(".git", "")
        owner, repo = path.split("/", 1)

        return f"https://github.com/{owner}/{repo}/commit/{commit_id}"

    except subprocess.CalledProcessError:
        return ""


def add_git_deletion_info(orphaned_tables, models_path="models"):
    """
    Adds git deletion info for orphaned tables if exists.
    """
    deleted_files = parse_git_log_output(models_path)
    choices = []

    for fq_table in orphaned_tables:
        _, _, table_name = fq_table.split(".")
        match = match_table_to_deletion(table_name, deleted_files)

        if match:
            formatted_time = format_deletion_timestamp(match["timestamp"])
            commit_url = get_github_commit_url(match["commit"])
            choices.append(
                {
                    "name": f"{fq_table:<95} {formatted_time:>5} | {match['commit']:>5}",
                    "value": fq_table,
                    "description": f"deleted by {match['author']} - {commit_url}",
                }
            )
        else:
            choices.append({"name": fq_table})

    return choices


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
        deletion_details = add_git_deletion_info(sorted(orphaned))
        # Prompt user to select tables to delete
        selected_tables = (
            multiselect_from_list(
                "Select orphaned tables to delete",
                items=deletion_details,
                allow_none=True,
            )
            or []
        )

        for table_id in selected_tables:
            project_name = table_id.split(".")[0]
            if force_delete and project_name not in (
                project_config().orphan_cleanup_skip_projects
            ):
                info("Can't force delete unless dev!", style="yellow")
                continue
            elif (
                target == Target.prod
                and project_name not in project_config().orphan_cleanup_projects
            ):
                info(
                    f"Can't delete table from project {project_name}. Must be one of {', '.join(project_config().orphan_cleanup_projects)}",
                    style="yellow",
                )
                continue
            client.delete_table(table_id=table_id)
            info(f"Deleted {table_id}.")
