import os
import shutil
import subprocess
import time
import webbrowser
from datetime import date, timedelta
from textwrap import dedent

from jinja2 import Template

from ..config.project import project_config, project_dbtwiz_path
from ..core.project import Profile
from ..dbt.run import get_selected_models
from ..integrations.gcp_auth import ensure_auth
from ..ui.interact import confirm
from ..utils.logger import debug, fatal, info, warn

MAX_CONCURRENT_TASKS = 8
YAML_FILE = project_dbtwiz_path("backfill-cloudrun.yaml")


def estimate_batch_size(
    models: list[dict],
    sample_date: date,
    default_batch_size: int,
    target_bytes: int,
) -> int:
    """Estimate a safe batch size by dry-running each incremental model for a single day.

    Compiles the model SQL against the prod target for sample_date, submits it to
    BigQuery as a dry-run to get bytes scanned, then calculates a batch size that
    keeps each task under target_bytes. Falls back to default_batch_size if estimation
    fails for all models.
    """
    from dbt.cli.main import dbtRunner

    from ..integrations.bigquery import GCP_LOCATION, BigQueryClient
    from ..utils.contextmanagers import suppress_output

    profile = Profile().profile_config("prod")
    execution_project = profile.get("execution_project")
    client = BigQueryClient(default_project=execution_project)
    project_root = project_config().root_path()
    sample = sample_date.isoformat()
    min_batch_size = None

    for model in models:
        model_name = model["name"]
        table = model.get("alias") or model_name

        try:
            dbt_args = [
                "compile",
                "--select", model_name,
                "--exclude", "tag:no_backfill",
                "--target", "prod",
                "--project-dir", str(project_root),
                "--vars", f'{{data_interval_start: "{sample}", data_interval_end: "{sample}", is_backfill: true}}',
            ]
            with suppress_output():
                result = dbtRunner().invoke(dbt_args)

            if not result.success:
                warn(f"Failed to compile {table} for batch size estimation, auto-sizing skipped")
                continue

            compiled_files = list(
                (project_root / "target" / "compiled").glob(f"**/{model_name}.sql")
            )
            if not compiled_files:
                warn(f"Compiled SQL not found for {table}, auto-sizing skipped")
                continue

            sql = compiled_files[0].read_text(encoding="utf-8")

            bq = client.get_bigquery()
            job_config = bq.QueryJobConfig(dry_run=True, use_query_cache=False)
            job = client.get_client().query(sql, job_config=job_config, location=GCP_LOCATION)

            bytes_per_day = job.total_bytes_processed
            if not bytes_per_day:
                debug(f"Dry-run returned 0 bytes for {table}, skipping")
                continue

            batch_size = min(default_batch_size, max(1, int(target_bytes / bytes_per_day)))
            info(
                f"Model {table}: ~{bytes_per_day / 1e9:.2f} GB/day scanned (estimate) → "
                f"batch size {batch_size} days (target {target_bytes / 1e9:.0f} GB/batch)"
            )

            if min_batch_size is None or batch_size < min_batch_size:
                min_batch_size = batch_size

        except Exception as e:
            warn(f"Failed to estimate batch size for {table}, auto-sizing skipped: {e}")

    if min_batch_size is None:
        info(f"No batch size estimate available, using default: {default_batch_size} days")
        return default_batch_size

    return min_batch_size


def chunk_date_range(
    first: date, last: date, batch_size: int
) -> list[tuple[date, date]]:
    """Split [first, last] (inclusive) into contiguous chunks of at most batch_size days."""
    ranges: list[tuple[date, date]] = []
    cursor = first
    while cursor <= last:
        chunk_end = min(last, cursor + timedelta(days=batch_size - 1))
        ranges.append((cursor, chunk_end))
        cursor = chunk_end + timedelta(days=1)
    return ranges


def encode_task_ranges(ranges: list[tuple[date, date]]) -> str:
    """Encode date-pair ranges into the --task-ranges arg format."""
    return ",".join(f"{s.isoformat()}:{e.isoformat()}" for s, e in ranges)


def decode_task_ranges(encoded: str) -> list[tuple[date, date]]:
    """Inverse of encode_task_ranges."""
    out: list[tuple[date, date]] = []
    for piece in encoded.split(","):
        piece = piece.strip()
        if not piece:
            continue
        s, e = piece.split(":")
        out.append((date.fromisoformat(s.strip()), date.fromisoformat(e.strip())))
    return out


def halve_str(word: str) -> str:
    """Halve a string by keeping the first and last quarter of the string"""
    word_len = len(word)
    word_len_quart = max(1, word_len // 4)
    first_quart = word[:word_len_quart]
    from_idx = word_len - word_len_quart
    last_quart = word[from_idx:]
    return f"{first_quart}{last_quart}"


def backfill_job_name(selector: str) -> str:
    """Generate job name based on the given dbt selector"""
    max_len = 64
    name = selector.replace("_", "-").replace("+", "")
    while len(name) > max_len:
        prev_len = len(name)
        words = name.split("-")
        longest_word = max(words, key=len)
        longest_word_idx = words.index(longest_word)
        half_word = halve_str(longest_word)
        words[longest_word_idx] = half_word
        # Remove empty words
        words = [w for w in words if len(w) > 0]
        name = "-".join(words)
        if len(name) == prev_len:
            # Couldn't shorten the name further by halving the longest word
            # so remove the last word
            words.pop()
            name = "-".join(words)
    return name


def job_timeout(target_name="prod", default=900, leeway=60) -> int:
    """Get job timeout from profile and add some leeway"""
    try:
        profile = Profile().profile_config(target_name)
        timeout = int(profile["job_execution_timeout_seconds"])
    except RuntimeError:
        timeout = default  # Default if unable to get from profile
    return timeout + leeway


def job_spec_template():
    """Templated YAML for Cloud Run job specification"""
    yaml = """
    apiVersion: run.googleapis.com/v1
    kind: Job
    metadata:
      name: {{ job_name }}
      labels:
        cloud.googleapis.com/location: {{ service_account_region }}
    spec:
      template:
        spec:
          parallelism: {{ parallelism }}
          taskCount: {{ task_count }}
          template:
            spec:
              containers:
              - image: {{ image }}
                command:
                - dbtwiz
                args:
                - build
                - --target
                - "prod"
                - --select
                - "{{ selector }}"
                - --task-ranges
                - "{{ task_ranges }}"
                - --use-task-index
                - --backfill
                {% if full_refresh %}
                - --full-refresh
                {%- endif %}
                resources:
                  limits:
                    cpu: 1000m
                    memory: 2Gi
              maxRetries: 0
              timeoutSeconds: {{ job_timeout }}
              serviceAccountName: {{ service_account }}
    """
    return Template(dedent(yaml).lstrip("\n"))


def generate_job_spec(
    selector: str,
    ranges: list[tuple[date, date]],
    full_refresh: bool,
    parallelism: int,
) -> str:
    """Generate job specification YAML file from an explicit list of per-task date ranges."""
    task_count = len(ranges)
    if task_count == 0:
        fatal("No date ranges to run.")
    parallelism = min(task_count, parallelism)
    job_name = backfill_job_name(selector)
    if full_refresh:
        assert task_count == 1
        first, last = ranges[0]
        assert first == last
        assert "+" not in selector
    job_spec_yaml = job_spec_template().render(
        job_name=job_name,
        parallelism=parallelism,
        task_count=task_count,
        image=project_config().docker_image_url_dbt,
        selector=selector,
        task_ranges=encode_task_ranges(ranges),
        full_refresh=full_refresh,
        service_account=project_config().service_account_identifier,
        service_account_region=project_config().service_account_region,
        job_timeout=job_timeout(),
    )
    with open(YAML_FILE, "w+", encoding="utf-8") as f:
        f.write(job_spec_yaml)
    return job_name


def run_command(args: list[str], verbose: bool = False, check: bool = True):
    """Run the given command in a subprocess"""
    if verbose:
        debug(f"Running command: {' '.join([str(arg) for arg in args])}")

    # Ensure all arguments are strings (handles Path objects)
    str_args = [str(arg) for arg in args]

    # On Windows, resolve the executable explicitly to avoid FileNotFoundError
    if os.name == "nt":
        executable = str_args[0]
        resolved_executable = (
            shutil.which(executable)
            or shutil.which(f"{executable}.cmd")
            or shutil.which(f"{executable}.exe")
        )
        if resolved_executable is None:
            raise FileNotFoundError(
                f"Executable '{executable}' not found on PATH. "
                "Ensure it is installed and available (e.g., install Google Cloud SDK to use 'gcloud')."
            )
        str_args[0] = resolved_executable

    result = subprocess.run(str_args, shell=False)
    if check:
        result.check_returncode()
    return result


def _cloud_run_session():
    """Return an AuthorizedSession for the Cloud Run Admin v2 REST API."""
    from google.auth import default
    from google.auth.transport.requests import AuthorizedSession

    credentials, _ = default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    return AuthorizedSession(credentials)


def _cloud_run_v2_base_url() -> str:
    region = project_config().service_account_region
    project = project_config().service_account_project
    return (
        f"https://{region}-run.googleapis.com/v2/projects/{project}/locations/{region}"
    )


def fetch_latest_execution(job_name: str, verbose: bool = False) -> dict:
    """Return the most recent Cloud Run v2 execution resource for the given job."""
    session = _cloud_run_session()
    list_url = f"{_cloud_run_v2_base_url()}/jobs/{job_name}/executions"
    if verbose:
        debug(f"GET {list_url}")
    resp = session.get(list_url, params={"pageSize": 1})
    if resp.status_code == 404:
        fatal(
            f"No Cloud Run job named '{job_name}' found. "
            "Has this backfill ever been run?"
        )
    if not resp.ok:
        fatal(
            f"Failed to list executions for '{job_name}': {resp.status_code} {resp.text}"
        )
    executions = resp.json().get("executions") or []
    if not executions:
        fatal(
            f"No previous executions found for job '{job_name}'. "
            "Run a fresh backfill before using --retry."
        )
    # The list endpoint already returns full execution resources, no need to re-fetch.
    return executions[0]


def extract_container_args(execution: dict) -> list[str]:
    """Extract the container args list from a Cloud Run v2 execution resource."""
    try:
        containers = execution["template"]["containers"]
        return list(containers[0].get("args", []))
    except (KeyError, IndexError) as exc:
        fatal(f"Could not read container args from previous execution: {exc}")


def get_arg_value(args: list[str], flag: str) -> str | None:
    """Return the value following the given flag in a list of CLI args, or None."""
    try:
        idx = args.index(flag)
    except ValueError:
        return None
    if idx + 1 >= len(args):
        return None
    return args[idx + 1]


def recover_previous_ranges(args: list[str]) -> list[tuple[date, date]]:
    """Recover the per-task date ranges from a previous execution's container args.

    Supports both new-style (--task-ranges) and old-style
    (--start-date/--end-date/--batch-size) executions.
    """
    encoded = get_arg_value(args, "--task-ranges")
    if encoded:
        return decode_task_ranges(encoded)

    start_str = get_arg_value(args, "--start-date")
    end_str = get_arg_value(args, "--end-date")
    bs_str = get_arg_value(args, "--batch-size")
    if not (start_str and end_str and bs_str):
        fatal(
            "Could not recover date ranges from previous execution: "
            "missing --task-ranges and --start-date/--end-date/--batch-size."
        )
    return chunk_date_range(
        date.fromisoformat(start_str), date.fromisoformat(end_str), int(bs_str)
    )


def extract_failed_task_indices(execution: dict, verbose: bool = False) -> list[int]:
    """Return the sorted list of zero-based task indices that did not succeed.

    Uses the Cloud Run v2 tasks endpoint to enumerate per-task state.
    """
    task_count = execution.get("taskCount")
    if task_count is None:
        fatal("Could not determine task count from previous execution.")
    succeeded = execution.get("succeededCount", 0) or 0
    if succeeded == task_count:
        return []

    # Derive the execution-relative path from its fully-qualified resource name:
    #   projects/.../locations/.../jobs/.../executions/<exec>
    execution_path = execution["name"]
    session = _cloud_run_session()
    tasks_url = f"https://{project_config().service_account_region}-run.googleapis.com/v2/{execution_path}/tasks"
    if verbose:
        debug(f"GET {tasks_url}")

    failed: list[int] = []
    next_page_token: str | None = None
    while True:
        params: dict[str, str] = {"pageSize": "500"}
        if next_page_token:
            params["pageToken"] = next_page_token
        resp = session.get(tasks_url, params=params)
        if not resp.ok:
            fatal(f"Failed to list tasks for execution: {resp.status_code} {resp.text}")
        payload = resp.json()
        for task in payload.get("tasks") or []:
            # Protobuf JSON omits zero-valued integers, so a missing 'index' means 0.
            index = int(task.get("index", 0))
            conditions = task.get("conditions") or []
            completed_ok = any(
                c.get("type") == "Completed" and c.get("state") == "CONDITION_SUCCEEDED"
                for c in conditions
            )
            if not completed_ok:
                failed.append(index)
        next_page_token = payload.get("nextPageToken")
        if not next_page_token:
            break
    return sorted(failed)


def subdivide_ranges(
    ranges: list[tuple[date, date]], batch_size: int
) -> list[tuple[date, date]]:
    """Re-chunk each range so no chunk exceeds batch_size days."""
    new_ranges: list[tuple[date, date]] = []
    for start, end in ranges:
        new_ranges.extend(chunk_date_range(start, end, batch_size))
    return new_ranges


def submit_job(job_name: str, verbose: bool, status: bool) -> None:
    """Replace and execute the Cloud Run job from the rendered YAML."""
    ensure_auth(check_app_default_auth=False, check_gcloud_auth=True)
    service_account_project = project_config().service_account_project
    service_account_region = project_config().service_account_region

    info("Preparing job for execution.")
    run_command(
        [
            "gcloud",
            "run",
            f"--project={service_account_project}",
            "jobs",
            "replace",
            YAML_FILE,
        ],
        verbose=verbose,
    )

    info("Starting job execution.")
    run_command(
        [
            "gcloud",
            "run",
            f"--project={service_account_project}",
            "jobs",
            "execute",
            f"--region={service_account_region}",
            job_name,
        ],
        verbose=verbose,
    )

    job_url = (
        f"https://console.cloud.google.com/run/jobs/details"
        f"/{service_account_region}/{job_name}/executions?project={service_account_project}"
    )
    info(f"Job status page: {job_url}")
    if status:
        webbrowser.open(job_url)
        time.sleep(0.5)


def _print_dry_run_summary(ranges: list[tuple[date, date]], job_name: str) -> None:
    info(f"Dry run — job '{job_name}' would submit {len(ranges)} task(s):")
    for i, (s, e) in enumerate(ranges):
        days = (e - s).days + 1
        info(f"  task {i:>3}: {s} → {e}  ({days} day{'s' if days > 1 else ''})")


def _backfill_retry(
    selector: str,
    parallelism: int,
    status: bool,
    verbose: bool,
    batch_size: int,
    dry_run: bool,
    batch_size_overridden: bool,
) -> None:
    """Retry failed tasks from the most recent execution."""
    job_name = backfill_job_name(selector)
    info(f"Looking up most recent execution of job '{job_name}'.")
    execution = fetch_latest_execution(job_name, verbose=verbose)

    if not execution.get("completionTime"):
        fatal(
            "Previous execution has not completed yet. "
            "Wait for it to finish before retrying."
        )

    previous_args = extract_container_args(execution)
    previous_ranges = recover_previous_ranges(previous_args)
    failed_indices = extract_failed_task_indices(execution, verbose=verbose)

    if not failed_indices:
        info("No failed tasks in the most recent execution; nothing to retry.")
        return

    failed_ranges = [previous_ranges[i] for i in failed_indices]
    info(
        f"Found {len(failed_ranges)} failed task(s) out of "
        f"{len(previous_ranges)} in the previous execution."
    )

    if batch_size_overridden:
        ranges = subdivide_ranges(failed_ranges, batch_size)
        info(
            f"Subdividing failed ranges with batch size {batch_size}: "
            f"{len(failed_ranges)} → {len(ranges)} tasks."
        )
    else:
        ranges = failed_ranges

    if dry_run:
        _print_dry_run_summary(ranges, job_name)
        return

    previous_full_refresh = "--full-refresh" in previous_args
    generate_job_spec(
        selector=selector,
        ranges=ranges,
        full_refresh=previous_full_refresh,
        parallelism=parallelism,
    )
    submit_job(job_name, verbose=verbose, status=status)


def backfill(
    selector: str,
    first_date: date | None,
    last_date: date | None,
    full_refresh: bool,
    parallelism: int,
    status: bool,
    verbose: bool,
    batch_size: int,
    retry: bool = False,
    dry_run: bool = False,
    batch_size_overridden: bool = False,
):
    """Runs backfill for the given selector and date interval, or retries failed tasks."""
    ensure_auth()

    if retry:
        _backfill_retry(
            selector=selector,
            parallelism=parallelism,
            status=status,
            verbose=verbose,
            batch_size=batch_size,
            dry_run=dry_run,
            batch_size_overridden=batch_size_overridden,
        )
        return

    selected_models = get_selected_models(select=selector)
    if len(selected_models) == 0:
        fatal(
            f"No models selected by statement '{selector}'. Please check the model name(s)."
        )

    materialized_counts = {}
    for item in selected_models:
        key = item["config"]["materialized"]
        materialized_counts[key] = materialized_counts.get(key, 0) + 1

    if materialized_counts.get("incremental", 0) == 0:
        warn("No incremental models were selected, so provided dates will be ignored.")
        if not confirm("Would you still like to run?"):
            return
        first_date = last_date = date.today()
    elif not batch_size_overridden:
        incremental_models = [
            m for m in selected_models if m["config"]["materialized"] == "incremental"
        ]
        target_bytes = project_config().backfill_max_bytes_per_batch_gb * 10**9
        batch_size = estimate_batch_size(
            models=incremental_models,
            sample_date=last_date,
            default_batch_size=batch_size,
            target_bytes=target_bytes,
        )

    ranges = chunk_date_range(first_date, last_date, batch_size)
    job_name = backfill_job_name(selector)

    if dry_run:
        _print_dry_run_summary(ranges, job_name)
        return

    job_name = generate_job_spec(
        selector=selector,
        ranges=ranges,
        full_refresh=full_refresh,
        parallelism=parallelism,
    )
    submit_job(job_name, verbose=verbose, status=status)
