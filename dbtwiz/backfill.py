from datetime import date
import time
import subprocess
import webbrowser

from jinja2 import Template
from textwrap import dedent

from .auth import ensure_auth
from .config import project_config, project_dbtwiz_path
from .logging import debug, info


class Backfill:
    """Backfill dbt models using Cloud Run with latest image built from master branch."""

    MAX_CONCURRENT_TASKS = 8
    YAML_FILE = project_dbtwiz_path("backfill-cloudrun.yaml")


    @classmethod
    def backfill_job_name(cls, selector: str) -> str:
        """Generate job name based on the given dbt selector"""
        name = selector.replace('_', '-').replace('+', '')
        return f"dbt-backfill-{name}"


    @classmethod
    def generate_job_spec(
            cls,
            selector: str,
            date_first: date,
            date_last: date,
            full_refresh: bool,
            parallelism: int,
    ) -> str:
        """Generate job specification YAML file"""
        number_of_days = (date_last - date_first).days + 1
        parallelism = min(number_of_days, parallelism)
        job_name = cls.backfill_job_name(selector)
        if full_refresh:
            assert number_of_days == 1
            assert '+' not in selector
        job_spec_yaml = cls.job_spec_template().render(
            job_name=job_name,
            parallelism=parallelism,
            task_count=number_of_days,
            image=project_config().dbt_image_url,
            selector=selector,
            start_date=date_first.strftime("%Y-%m-%d"),
            full_refresh=full_refresh,
            service_account=project_config().dbt_service_account,
            gcp_region=project_config().gcp_region,
        )
        with open(cls.YAML_FILE, "w+") as f:
            f.write(job_spec_yaml)
        return job_name


    @classmethod
    def job_spec_template(cls):
        """Templated YAML for Cloud Run job specification"""
        yaml = """
        apiVersion: run.googleapis.com/v1
        kind: Job
        metadata:
          name: {{ job_name }}
          labels:
            cloud.googleapis.com/location: {{ gcp_region }}
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
                    - --date
                    - "{{ start_date }}"
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


    @classmethod
    def run_command(cls, args: list[str], verbose: bool = False, check: bool = True):
        """Run the given command in a subprocess"""
        if verbose:
            debug(f"Running command: {' '.join(args)}")
        result = subprocess.run(args)
        if check:
            result.check_returncode()
        return result


    @classmethod
    def run(
            cls,
            selector: str,
            first_date: date,
            last_date: date,
            full_refresh: bool,
            parallelism: int,
            status: bool,
            verbose: bool
    ):
        job_name = cls.generate_job_spec(
            selector=selector,
            date_first=first_date,
            date_last=last_date,
            full_refresh=full_refresh,
            parallelism=parallelism)

        ensure_auth()

        gcp_project = project_config().gcp_project
        gcp_region = project_config().gcp_region

        info("Preparing job for execution.")
        cls.run_command(
            ["gcloud", "run", f"--project={gcp_project}",
             "jobs", "replace", cls.YAML_FILE],
            verbose=verbose)

        info("Starting job execution.")
        cls.run_command(
            ["gcloud", "run", f"--project={gcp_project}",
             "jobs", "execute", f"--region={gcp_region}", job_name],
            verbose=verbose)

        job_url = (
            f"https://console.cloud.google.com/run/jobs/details"
            f"/{gcp_region}/{job_name}/executions?project={gcp_project}"
        )
        info(f"Job status page: {job_url}")
        if status:
            webbrowser.open(job_url)
            time.sleep(0.5)
