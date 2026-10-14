"""Incidents read from Elementary's run history, shared by `incident-alert` and
`incident-report`.

An incident is a model's unbroken streak of failed runs within one orchestrator job.
Elementary already uploads every run's results (`dbt_invocations`, `dbt_run_results`),
so incidents are derived from those on demand and nothing of our own is stored.

Runs are told apart by Elementary's orchestrator columns (`job_id`, `job_run_id`), which
the project fills in from its own dbt vars; invocations without them (manual runs) are
ignored.
"""

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone, tzinfo
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set

from ..config.project import project_config
from ..utils.logger import fatal, warn

ERROR_STATUSES = ("error", "runtime error")

# Downstream nodes that hold data; tests and unit tests below a failing model are not
# part of what it takes down.
DATA_NODE_PREFIXES = ("model.", "snapshot.", "seed.")

# A model's status in a run is its status in the run's last invocation that included
# it, so a `dbt retry` that succeeds clears the build it retried. Airflow run ids repeat
# across DAGs, so a run is (job_id, job_run_id), never the run id alone.
INCIDENTS_SQL = """
with
    invocations as (
        select
            invocation_id,
            job_id,
            job_run_id,
            safe_cast(run_started_at as timestamp) as run_start_ts,
        from `{dataset}.dbt_invocations`
        where
            job_id is not null
            and job_run_id is not null
            and safe_cast(run_started_at as timestamp)
            between timestamp_sub(@as_of, interval @window_days day) and @as_of
    ),

    runs as (
        select job_id, job_run_id, min(run_start_ts) as run_start_ts,
        from invocations
        group by job_id, job_run_id
    ),

    latest_runs as (
        select
            job_id,
            array_agg(job_run_id order by run_start_ts desc limit 1)[offset(0)]
            as job_run_id,
        from runs
        group by job_id
    ),

    node_runs as (
        select
            i.job_id,
            i.job_run_id,
            r.unique_id,
            r.name,
            lower(r.status) as status,
            r.message,
        from `{dataset}.dbt_run_results` as r
        inner join invocations as i using (invocation_id)
        where
            r.resource_type in ('model', 'snapshot', 'seed')
            and r.created_at between timestamp_sub(@as_of, interval @window_days + 1 day)
            and timestamp_add(@as_of, interval 1 day)
        qualify
            row_number() over (
                partition by i.job_id, i.job_run_id, r.unique_id
                order by i.run_start_ts desc
            )
            = 1
    ),

    -- Skipped runs say nothing about the model itself, so they neither extend nor end
    -- a streak. Counting the clean runs so far gives every run in a streak the same id.
    flagged as (
        select
            n.job_id,
            n.job_run_id,
            n.unique_id,
            n.name,
            n.message,
            ru.run_start_ts,
            n.status in unnest(@error_statuses) as is_error,
            countif(n.status not in unnest(@error_statuses)) over (
                partition by n.job_id, n.unique_id
                order by ru.run_start_ts
                rows between unbounded preceding and current row
            ) as streak_id,
        from node_runs as n
        inner join runs as ru using (job_id, job_run_id)
        where n.status != 'skipped'
    ),

    incidents as (
        select
            job_id,
            unique_id,
            streak_id,
            any_value(name) as name,
            min(run_start_ts) as first_failed_ts,
            max(run_start_ts) as last_failed_ts,
            count(*) as failed_run_count,
            array_agg(
                struct(job_run_id, run_start_ts, message)
                order by run_start_ts desc
                limit 2
            ) as last_failures,
        from flagged
        where is_error
        group by job_id, unique_id, streak_id
    ),

    -- The clean run that ends streak k is the first one counted as k + 1.
    clears as (
        select
            job_id,
            unique_id,
            streak_id - 1 as streak_id,
            run_start_ts as cleared_ts,
            job_run_id as cleared_run_id,
        from flagged
        where not is_error
    )

select
    i.job_id,
    i.unique_id,
    i.name,
    i.first_failed_ts,
    i.last_failed_ts,
    i.failed_run_count,
    i.streak_id = 0 as is_start_truncated,
    i.last_failures[offset(0)].message as last_message,
    i.last_failures[safe_offset(1)].run_start_ts as previous_failed_ts,
    c.cleared_ts,
    coalesce(i.last_failures[offset(0)].job_run_id = l.job_run_id, false)
    as is_failed_in_latest_run,
    coalesce(c.cleared_run_id = l.job_run_id, false) as is_cleared_in_latest_run,
from incidents as i
left join clears as c using (job_id, unique_id, streak_id)
left join latest_runs as l using (job_id)
"""


@dataclass(frozen=True)
class Incident:
    """One model's unbroken streak of failed runs within one job.

    Attributes:
        job_id: Orchestrator job (the Airflow DAG) the runs belong to
        unique_id: dbt unique_id of the failing node
        name: Node name
        first_failed_ts: Start of the first failed run in the streak
        last_failed_ts: Start of the latest failed run
        failed_run_count: Number of failed runs in the streak
        is_start_truncated: True when no clean run precedes the streak inside the
            window, so it may have started earlier than first_failed_ts
        last_message: Error message from the latest failed run
        previous_failed_ts: Start of the failed run before the latest, if any
        cleared_ts: Start of the clean run that ended the streak, None while open
        is_failed_in_latest_run: The job's latest run is the latest failed run
        is_cleared_in_latest_run: The job's latest run is the one that cleared it
    """

    job_id: str
    unique_id: str
    name: str
    first_failed_ts: datetime
    last_failed_ts: datetime
    failed_run_count: int
    is_start_truncated: bool
    last_message: Optional[str]
    previous_failed_ts: Optional[datetime]
    cleared_ts: Optional[datetime]
    is_failed_in_latest_run: bool
    is_cleared_in_latest_run: bool

    @property
    def is_open(self) -> bool:
        return self.cleared_ts is None

    @property
    def failing_for(self) -> timedelta:
        """Time from the first to the latest failed run."""
        return self.last_failed_ts - self.first_failed_ts


@dataclass
class BlastRadius:
    """Everything downstream of a set of failing nodes, whatever any run selected.

    Attributes:
        downstream: Models, snapshots and seeds below the failing nodes
        shared: The downstream nodes with `access` public or protected, i.e. the ones
            other groups build on
        exposures: Exposures below the failing nodes
    """

    downstream: Set[str] = field(default_factory=set)
    shared: Set[str] = field(default_factory=set)
    exposures: Set[str] = field(default_factory=set)


@dataclass
class ManifestGraph:
    """The parts of a manifest needed to work out a blast radius."""

    child_map: Dict[str, List[str]]
    access: Dict[str, str]
    names: Dict[str, str]

    @classmethod
    def load(cls, path: Path) -> "ManifestGraph":
        """Read the manifest; there is no blast radius without one, so it's required."""
        try:
            with open(path, "r", encoding="utf-8") as f:
                manifest = json.load(f)
        except (OSError, json.JSONDecodeError) as e:
            fatal(f"Could not read manifest {path}: {e}")
        nodes = manifest.get("nodes", {})
        exposures = manifest.get("exposures", {})
        return cls(
            child_map=manifest.get("child_map", {}),
            access={uid: node.get("access") or "" for uid, node in nodes.items()},
            names={
                **{uid: node.get("name", uid) for uid, node in nodes.items()},
                **{
                    uid: exp.get("label") or exp.get("name", uid)
                    for uid, exp in exposures.items()
                },
            },
        )

    def descendants(self, unique_id: str) -> Set[str]:
        """Every node reachable downstream of unique_id, excluding itself."""
        seen: Set[str] = set()
        stack = list(self.child_map.get(unique_id, []))
        while stack:
            uid = stack.pop()
            if uid in seen:
                continue
            seen.add(uid)
            stack.extend(self.child_map.get(uid, []))
        return seen

    def has_node(self, unique_id: str) -> bool:
        return unique_id in self.access

    def name(self, unique_id: str) -> str:
        return self.names.get(unique_id, unique_id.split(".")[-1])

    def sorted_names(self, unique_ids: Iterable[str]) -> List[str]:
        return sorted(self.name(uid) for uid in unique_ids)


def blast_radius(incidents: Iterable[Incident], graph: ManifestGraph) -> BlastRadius:
    """Everything below a set of failing nodes, other than the failing nodes themselves.

    This is the lineage, not what a run skipped: a run only selects some of what is
    downstream, and the rest goes stale just the same.
    """
    failing = {i.unique_id for i in incidents}
    below: Set[str] = set()
    for uid in failing:
        below |= graph.descendants(uid)
    below -= failing
    downstream = {uid for uid in below if uid.startswith(DATA_NODE_PREFIXES)}
    return BlastRadius(
        downstream=downstream,
        shared={
            uid
            for uid in downstream
            if graph.access.get(uid) in ("public", "protected")
        },
        exposures={uid for uid in below if uid.startswith("exposure.")},
    )


def drop_removed_nodes(
    incidents: List[Incident], graph: ManifestGraph
) -> List[Incident]:
    """Drop open incidents of nodes the manifest no longer has.

    A deleted or disabled model never runs again, so nothing would ever clear it.
    """
    return [i for i in incidents if not i.is_open or graph.has_node(i.unique_id)]


# Lines that only say which node failed; the cause is on the line after them.
_LOCATION_LINE = re.compile(r"^(Database|Runtime|Compilation|Parsing) Error in ", re.I)
_ADAPTER_PREFIX = re.compile(r"^\[\w+\]\s*(googleapi:\s*)?")
# BigQuery appends a console link to the failed job, which differs for every run.
_QUERY_LINK = re.compile(r",?\s*\(Query: https?://\S+\)\s*$")


def summarize_cause(message: Optional[str], limit: int = 300) -> str:
    """Reduce a dbt error message to the line that says what went wrong.

    dbt prefixes the cause with a line naming the failing node and its compiled path,
    which differs for every node; dropping it lets failures with one cause group.
    """
    if not message:
        return "No error message"
    lines = [line.strip() for line in message.splitlines() if line.strip()]
    causes = [line for line in lines if not _LOCATION_LINE.match(line)]
    cause = _QUERY_LINK.sub("", _ADAPTER_PREFIX.sub("", (causes or lines)[0]))
    return cause if len(cause) <= limit else cause[: limit - 1] + "…"


def group_by_cause(incidents: Iterable[Incident]) -> Dict[str, List[Incident]]:
    """Group incidents by their summarized cause, longest-failing group first."""
    groups: Dict[str, List[Incident]] = {}
    for incident in incidents:
        groups.setdefault(summarize_cause(incident.last_message), []).append(incident)
    for members in groups.values():
        members.sort(key=lambda i: i.failing_for, reverse=True)
    return dict(sorted(groups.items(), key=lambda g: g[1][0].failing_for, reverse=True))


def format_duration(duration: timedelta) -> str:
    """Render a duration rounded to its largest units, e.g. `45m`, `1h 30m`, `6h`, `2d 3h`.

    Rounded, not truncated: a run that crossed the 6h threshold at 5h59m reads `6h`.
    """
    minutes = round(duration.total_seconds() / 60)
    if minutes < 60:
        return f"{minutes}m"
    if minutes < 120:
        return f"1h {minutes - 60}m" if minutes > 60 else "1h"
    hours = round(minutes / 60)
    if hours < 24:
        return f"{hours}h"
    days, hours = divmod(hours, 24)
    return f"{days}d {hours}h" if hours else f"{days}d"


def format_ts(ts: datetime, tz: tzinfo) -> str:
    return ts.astimezone(tz).strftime("%Y-%m-%d %H:%M")


def display_timezone() -> tzinfo:
    """The zone times are shown in, from `incidents_timezone`."""
    name = project_config().incidents_timezone
    if not name or name == "UTC":
        return timezone.utc
    try:
        from zoneinfo import ZoneInfo

        return ZoneInfo(name)
    except Exception:
        warn(f"Unknown incidents_timezone '{name}'; showing times in UTC.")
        return timezone.utc


def fetch_incidents(as_of: datetime) -> List[Incident]:
    """Query Elementary's run history for incidents in the window ending at as_of."""
    from ..integrations.bigquery import GCP_LOCATION, BigQueryClient

    config = project_config()
    dataset = config.incidents_elementary_dataset
    if not dataset:
        fatal("Set incidents_elementary_dataset in [tool.dbtwiz.project].")

    client = BigQueryClient()
    bq = client.get_bigquery()
    job_config = bq.QueryJobConfig(
        query_parameters=[
            bq.ScalarQueryParameter("as_of", "TIMESTAMP", as_of),
            bq.ScalarQueryParameter(
                "window_days", "INT64", config.incidents_window_days
            ),
            bq.ArrayQueryParameter("error_statuses", "STRING", list(ERROR_STATUSES)),
        ]
    )
    rows = (
        client.get_client()
        .query(
            INCIDENTS_SQL.format(dataset=dataset),
            job_config=job_config,
            location=GCP_LOCATION,
        )
        .result()
    )
    return [
        Incident(
            job_id=r["job_id"],
            unique_id=r["unique_id"],
            name=r["name"],
            first_failed_ts=r["first_failed_ts"],
            last_failed_ts=r["last_failed_ts"],
            failed_run_count=r["failed_run_count"],
            is_start_truncated=r["is_start_truncated"],
            last_message=r["last_message"],
            previous_failed_ts=r["previous_failed_ts"],
            cleared_ts=r["cleared_ts"],
            is_failed_in_latest_run=r["is_failed_in_latest_run"],
            is_cleared_in_latest_run=r["is_cleared_in_latest_run"],
        )
        for r in rows
    ]
