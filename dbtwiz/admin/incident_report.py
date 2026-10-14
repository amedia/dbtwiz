"""Render a page of every job's open incidents and those cleared within the window."""

import re
from datetime import datetime, timedelta, timezone, tzinfo
from pathlib import Path
from typing import Any, Dict, List, Optional

from jinja2 import Environment

from ..config.project import project_config
from ..templates import path_to_template
from ..utils.logger import fatal, info
from .incidents import (
    Incident,
    ManifestGraph,
    blast_radius,
    display_timezone,
    drop_removed_nodes,
    fetch_incidents,
    format_duration,
    format_ts,
    summarize_cause,
)

# Must match the job-N colours defined in the template.
JOB_COLOR_COUNT = 6


def severity(failing_for: timedelta) -> str:
    """Colour band of an open incident: under 3h, 3-12h, 12h or more."""
    if failing_for >= timedelta(hours=12):
        return "high"
    if failing_for >= timedelta(hours=3):
        return "mid"
    return "low"


def render_report(
    incidents: List[Incident],
    graph: ManifestGraph,
    tz: tzinfo,
    generated_at: datetime,
    window_days: int,
) -> str:
    """Render the HTML page of open incidents and those cleared within the window."""
    # One colour per job, stable within the page.
    job_colors = {
        job: n % JOB_COLOR_COUNT
        for n, job in enumerate(sorted({i.job_id for i in incidents}))
    }

    def row(i: Incident) -> Dict[str, Any]:
        radius = blast_radius([i], graph)
        return {
            "job_id": i.job_id,
            "job_color": job_colors[i.job_id],
            "severity": severity(i.failing_for),
            "name": i.name,
            "first_failed": format_ts(i.first_failed_ts, tz),
            "is_start_truncated": i.is_start_truncated,
            "cleared": format_ts(i.cleared_ts, tz) if i.cleared_ts else None,
            "duration": format_duration(
                (i.cleared_ts or i.last_failed_ts) - i.first_failed_ts
            ),
            "failed_runs": i.failed_run_count,
            "cause": summarize_cause(i.last_message),
            "message": i.last_message or "",
            "downstream": graph.sorted_names(radius.downstream),
            "shared": graph.sorted_names(radius.shared),
            "exposures": graph.sorted_names(radius.exposures),
        }

    open_incidents = sorted(
        (i for i in incidents if i.is_open), key=lambda i: i.first_failed_ts
    )
    cleared = sorted(
        (i for i in incidents if not i.is_open),
        key=lambda i: i.cleared_ts,
        reverse=True,
    )
    template = Environment(autoescape=True).from_string(
        path_to_template("incident_report.html").read_text(encoding="utf-8")
    )
    return template.render(
        generated_at=format_ts(generated_at, tz),
        tz_name=str(tz),
        window_days=window_days,
        open_incidents=[row(i) for i in open_incidents],
        downstream_total=len(blast_radius(open_incidents, graph).downstream),
        cleared=[row(i) for i in cleared],
    )


def upload_report(uri: str, html: str) -> None:
    """Upload the page to a gs:// URI, uncached so a reload shows the latest run."""
    from google.cloud import storage

    match = re.match(r"^gs://([^/]+)/(.+)$", uri)
    if not match:
        raise ValueError(f"not a gs://bucket/path URI: {uri}")
    blob = storage.Client().bucket(match.group(1)).blob(match.group(2))
    blob.cache_control = "no-cache, max-age=0"
    blob.upload_from_string(html, content_type="text/html; charset=utf-8")


def incident_report(
    destination: Optional[str],
    manifest_path: Path,
    as_of: Optional[datetime],
) -> None:
    """Render the incidents page and write it to a gs:// URI or a local file.

    Args:
        destination: gs://bucket/path or a local path; defaults to incident_report_uri
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
    html = render_report(
        incidents, graph, display_timezone(), as_of, config.incidents_window_days
    )

    if destination.startswith("gs://"):
        try:
            upload_report(destination, html)
        except Exception as e:
            fatal(f"Uploading to {destination} failed: {e}")
    else:
        path = Path(destination)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(html, encoding="utf-8")
    open_count = sum(1 for i in incidents if i.is_open)
    info(
        f"Wrote {open_count} open and {len(incidents) - open_count} cleared incidents"
        f" to {destination}."
    )
