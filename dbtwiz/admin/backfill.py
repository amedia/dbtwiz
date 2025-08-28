import subprocess
import time
import webbrowser
from datetime import date
from math import ceil
from textwrap import dedent

from jinja2 import Template

from ..config.project import project_config, project_dbtwiz_path
from ..integrations.gcp_auth import ensure_auth
from ..utils.logger import debug, info

MAX_CONCURRENT_TASKS = 8
YAML_FILE = project_dbtwiz_path("backfill-cloudrun.yaml")


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


def generate_job_spec(
    selector: str,
    date_first: date,
    date_last: date,
    full_refresh: bool,
    parallelism: int,
    batch_size: int,
) -> str:
    """Generate job specification YAML file"""
    number_of_days = (date_last - date_first).days + 1
    task_count = ceil(number_of_days / batch_size)
    parallelism = min(task_count, parallelism)
    job_name = backfill_job_name(selector)
    if full_refresh:
        assert number_of_days == 1
        assert "+" not in selector
    job_spec_yaml = job_spec_template().render(
        job_name=job_name,
        parallelism=parallelism,
        task_count=task_count,
        image=project_config().docker_image_url_dbt,
        selector=selector,
        start_date=date_first.strftime("%Y-%m-%d"),
        end_date=date_last.strftime("%Y-%m-%d"),
        batch_size=batch_size,
        full_refresh=full_refresh,
        service_account=project_config().service_account_identifier,
        service_account_region=project_config().service_account_region,
    )
    with open(YAML_FILE, "w+") as f:
        f.write(job_spec_yaml)
    return job_name


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
                - --start-date
                - "{{ start_date }}"
                - --end-date
                - "{{ end_date }}"
                - --batch-size
                - "{{ batch_size }}"
                - --use-task-index
                {% if full_refresh %}
                - --full-refresh
                {%- endif %}
                resources:
                  limits:
                    cpu: 1000m
                    memory: 2Gi
              maxRetries: 2
              timeoutSeconds: 900
              serviceAccountName: {{ service_account }}
    """
    return Template(dedent(yaml).lstrip("\n"))


def run_command(args: list[str], verbose: bool = False, check: bool = True):
    """Run the given command in a subprocess"""
    if verbose:
        debug(f"Running command: {' '.join([str(arg) for arg in args])}")
    result = subprocess.run(args, shell=False)
    if check:
        result.check_returncode()
    return result


def backfill(
    selector: str,
    first_date: date,
    last_date: date,
    full_refresh: bool,
    parallelism: int,
    status: bool,
    verbose: bool,
    batch_size: int,
):
    """Runs backfill for the given selector and date interval."""
    job_name = generate_job_spec(
        selector=selector,
        date_first=first_date,
        date_last=last_date,
        full_refresh=full_refresh,
        parallelism=parallelism,
        batch_size=batch_size,
    )

    ensure_auth(check_app_default_auth=True, check_gcloud_auth=True)

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


def halve_str(word: str) -> str:
    """Halve a string by keeping the first and last quarter of the string"""
    word_len = len(word)
    word_len_quart = max(1, word_len // 4)
    first_quart = word[:word_len_quart]
    from_idx = word_len - word_len_quart
    last_quart = word[from_idx:]
    return f"{first_quart}{last_quart}"
