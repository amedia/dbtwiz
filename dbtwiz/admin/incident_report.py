"""Render a page of every job's status and incidents, with a JSON summary beside it.

The page shows each job's latest run (flagging one that has stopped running), its open
incidents with their blast radius, and those cleared within the window. The JSON file
carries the same status in a form an overview page can fetch.
"""

import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, tzinfo
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import quote

from jinja2 import Environment

from ..config.project import project_config
from ..templates import path_to_template
from ..utils.logger import fatal, info
from .incidents import (
    Incident,
    JobRun,
    ManifestGraph,
    blast_radius,
    display_timezone,
    drop_removed_nodes,
    fetch_incidents,
    fetch_job_runs,
    format_duration,
    format_ts,
    query_link,
    summarize_cause,
)

# Must match the job-N colours defined in the template.
JOB_COLOR_COUNT = 6


@dataclass(frozen=True)
class ReportLinks:
    """Where names on the page link to; a link is left out when its base is unset."""

    dbt_docs_url: Optional[str] = None
    airflow_url: Optional[str] = None

    def model(self, unique_id: str) -> Optional[str]:
        """The node's page in dbt docs, routed by resource type (`#!/model/…`)."""
        if not self.dbt_docs_url:
            return None
        resource_type = unique_id.split(".")[0]
        return f"{self.dbt_docs_url}#!/{resource_type}/{unique_id}"

    def job(self, job_id: str) -> Optional[str]:
        if not self.airflow_url:
            return None
        return f"{self.airflow_url.rstrip('/')}/dags/{quote(job_id, safe='')}"


def severity(failing_for: timedelta) -> str:
    """Colour band of an open incident: under 3h, 3-12h, 12h or more."""
    if failing_for >= timedelta(hours=12):
        return "high"
    if failing_for >= timedelta(hours=3):
        return "mid"
    return "low"


def _iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).isoformat()


def status_summary(
    incidents: List[Incident],
    job_runs: List[JobRun],
    generated_at: datetime,
    window_days: int,
    report_url: Optional[str],
) -> Dict[str, Any]:
    """The report's status as plain data, for an overview page to fetch."""
    open_incidents = [i for i in incidents if i.is_open]
    return {
        "generated_at": _iso(generated_at),
        "window_days": window_days,
        "report_url": report_url,
        "open_incident_count": len(open_incidents),
        "cleared_incident_count": len(incidents) - len(open_incidents),
        "jobs": [
            {
                "job_id": run.job_id,
                "last_run_ts": _iso(run.last_run_ts),
                "typical_interval_minutes": (
                    round(run.typical_interval.total_seconds() / 60)
                    if run.typical_interval
                    else None
                ),
                "is_overdue": run.is_overdue(generated_at),
                "open_incident_count": sum(
                    1 for i in open_incidents if i.job_id == run.job_id
                ),
            }
            for run in sorted(job_runs, key=lambda r: r.job_id)
        ],
        "open_incidents": [
            {
                "job_id": i.job_id,
                "unique_id": i.unique_id,
                "first_failed_ts": _iso(i.first_failed_ts),
                "failed_run_count": i.failed_run_count,
                "cause": summarize_cause(i.last_message),
            }
            for i in sorted(open_incidents, key=lambda i: i.first_failed_ts)
        ],
    }


def render_report(
    incidents: List[Incident],
    job_runs: List[JobRun],
    graph: ManifestGraph,
    tz: tzinfo,
    generated_at: datetime,
    window_days: int,
    stale_minutes: int = 90,
    links: ReportLinks = ReportLinks(),
) -> str:
    """Render the HTML page of job status, open incidents and recently cleared ones."""
    jobs = sorted({i.job_id for i in incidents} | {r.job_id for r in job_runs})
    job_colors = {job: n % JOB_COLOR_COUNT for n, job in enumerate(jobs)}
    open_incidents = sorted(
        (i for i in incidents if i.is_open), key=lambda i: i.first_failed_ts
    )
    cleared = sorted(
        (i for i in incidents if not i.is_open),
        key=lambda i: i.cleared_ts,
        reverse=True,
    )

    def row(i: Incident) -> Dict[str, Any]:
        radius = blast_radius([i], graph)
        return {
            "job_id": i.job_id,
            "job_color": job_colors[i.job_id],
            "job_url": links.job(i.job_id),
            "severity": severity(i.failing_for),
            "name": i.name,
            "model_url": links.model(i.unique_id),
            "first_failed": format_ts(i.first_failed_ts, tz),
            "is_start_truncated": i.is_start_truncated,
            "cleared": format_ts(i.cleared_ts, tz) if i.cleared_ts else None,
            "duration": format_duration(
                (i.cleared_ts or i.last_failed_ts) - i.first_failed_ts
            ),
            "failed_runs": i.failed_run_count,
            "cause": summarize_cause(i.last_message),
            "query_url": query_link(i.last_message),
            "message": i.last_message or "",
            "downstream": graph.sorted_names(radius.downstream),
            "shared": graph.sorted_names(radius.shared),
            "exposures": graph.sorted_names(radius.exposures),
        }

    def job_card(run: JobRun) -> Dict[str, Any]:
        limit = run.overdue_after()
        return {
            "job_id": run.job_id,
            "job_color": job_colors[run.job_id],
            "job_url": links.job(run.job_id),
            "last_run": format_ts(run.last_run_ts, tz),
            "last_run_iso": _iso(run.last_run_ts),
            "interval": format_duration(run.typical_interval)
            if run.typical_interval
            else None,
            "overdue_after_seconds": int(limit.total_seconds()) if limit else "",
            "is_overdue": run.is_overdue(generated_at),
            "open_count": sum(1 for i in open_incidents if i.job_id == run.job_id),
        }

    template = Environment(autoescape=True).from_string(
        path_to_template("incident_report.html").read_text(encoding="utf-8")
    )
    return template.render(
        generated_at=format_ts(generated_at, tz),
        generated_at_iso=_iso(generated_at),
        stale_seconds=stale_minutes * 60,
        tz_name=str(tz),
        window_days=window_days,
        jobs=[job_card(r) for r in sorted(job_runs, key=lambda r: r.job_id)],
        open_incidents=[row(i) for i in open_incidents],
        downstream_total=len(blast_radius(open_incidents, graph).downstream),
        cleared=[row(i) for i in cleared],
    )


def summary_destination(destination: str) -> str:
    """Where the JSON summary goes: beside the page, with a .json extension."""
    stem = (
        destination[: -len(".html")] if destination.endswith(".html") else destination
    )
    return f"{stem}.json"


def upload(uri: str, content: str, content_type: str) -> None:
    """Upload to a gs:// URI, uncached so a reload shows the latest run."""
    from google.cloud import storage

    match = re.match(r"^gs://([^/]+)/(.+)$", uri)
    if not match:
        raise ValueError(f"not a gs://bucket/path URI: {uri}")
    blob = storage.Client().bucket(match.group(1)).blob(match.group(2))
    blob.cache_control = "no-cache, max-age=0"
    blob.upload_from_string(content, content_type=content_type)


def _write(destination: str, content: str, content_type: str) -> None:
    if destination.startswith("gs://"):
        try:
            upload(destination, content, content_type)
        except Exception as e:
            fatal(f"Uploading to {destination} failed: {e}")
    else:
        path = Path(destination)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")


def incident_report(
    destination: Optional[str],
    manifest_path: Path,
    as_of: Optional[datetime],
) -> None:
    """Render the page and its JSON summary, and write both to gs:// or local files.

    Args:
        destination: gs://bucket/path or a local path; defaults to incident_report_uri.
            The summary goes beside it, with a .json extension.
        manifest_path: Manifest the blast radius is read from
        as_of: Render the history as it stood at this time
    """
    config = project_config()
    destination = destination or config.incident_report_uri
    if not destination:
        fatal("Pass --destination or set incident_report_uri in [tool.dbtwiz.project].")

    graph = ManifestGraph.load(manifest_path)
    as_of = as_of or datetime.now(timezone.utc)
    incidents = drop_removed_nodes(fetch_incidents(as_of), graph)
    job_runs = fetch_job_runs(as_of)
    window_days = config.incidents_window_days

    html = render_report(
        incidents,
        job_runs,
        graph,
        display_timezone(),
        as_of,
        window_days,
        config.incident_report_stale_minutes,
        ReportLinks(
            dbt_docs_url=config.incident_report_dbt_docs_url,
            airflow_url=config.incident_report_airflow_url,
        ),
    )
    summary = status_summary(
        incidents, job_runs, as_of, window_days, config.incident_report_url
    )
    _write(destination, html, "text/html; charset=utf-8")
    _write(
        summary_destination(destination),
        json.dumps(summary, indent=2),
        "application/json; charset=utf-8",
    )
    open_count = sum(1 for i in incidents if i.is_open)
    info(
        f"Wrote {open_count} open and {len(incidents) - open_count} cleared incidents"
        f" for {len(job_runs)} jobs to {destination} and its .json summary."
    )
