"""Incidents read from Elementary's run history, shared by `incident-alert` and
`incident-report`.

An incident is a node's unbroken streak of failed runs within one orchestrator job, where
a node is a model, snapshot, seed or data test.
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

# `fail` is a data test that found rows. A test with `warn` status was configured not to
# fail, so it counts as clean.
FAILURE_STATUSES = ("error", "runtime error", "fail")
RESOURCE_TYPES = ("model", "snapshot", "seed", "test")

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
            r.resource_type,
            lower(r.status) as status,
            r.message,
            r.failures,
        from `{dataset}.dbt_run_results` as r
        inner join invocations as i using (invocation_id)
        where
            r.resource_type in unnest(@resource_types)
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
            n.resource_type,
            n.status,
            n.message,
            n.failures,
            ru.run_start_ts,
            n.status in unnest(@failure_statuses) as is_failed,
            countif(n.status not in unnest(@failure_statuses)) over (
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
            any_value(resource_type) as resource_type,
            min(run_start_ts) as first_failed_ts,
            max(run_start_ts) as last_failed_ts,
            count(*) as failed_run_count,
            array_agg(
                struct(job_run_id, run_start_ts, status, message, failures)
                order by run_start_ts desc
                limit 2
            ) as last_failures,
        from flagged
        where is_failed
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
        where not is_failed
    )

select
    i.job_id,
    i.unique_id,
    i.name,
    i.resource_type,
    i.first_failed_ts,
    i.last_failed_ts,
    i.failed_run_count,
    i.streak_id = 0 as is_start_truncated,
    i.last_failures[offset(0)].status as last_status,
    i.last_failures[offset(0)].message as last_message,
    i.last_failures[offset(0)].failures as last_failure_count,
    i.last_failures[safe_offset(1)].run_start_ts as previous_failed_ts,
    c.cleared_ts,
    coalesce(i.last_failures[offset(0)].job_run_id = l.job_run_id, false)
    as is_failed_in_latest_run,
    coalesce(c.cleared_run_id = l.job_run_id, false) as is_cleared_in_latest_run,
from incidents as i
left join clears as c using (job_id, unique_id, streak_id)
left join latest_runs as l using (job_id)
"""

# Each job's latest run, and the median gap between its recent runs as the interval it
# normally runs at; a job that stops running shows up as overdue against it.
JOB_RUNS_SQL = """
with
    runs as (
        select
            job_id,
            job_run_id,
            min(safe_cast(run_started_at as timestamp)) as run_start_ts,
        from `{dataset}.dbt_invocations`
        where
            job_id is not null
            and job_run_id is not null
            and created_at >= timestamp_sub(@as_of, interval @window_days + 1 day)
            and safe_cast(run_started_at as timestamp)
            between timestamp_sub(@as_of, interval @window_days day) and @as_of
        group by job_id, job_run_id
    ),

    gaps as (
        select
            job_id,
            run_start_ts,
            timestamp_diff(
                run_start_ts,
                lag(run_start_ts) over (partition by job_id order by run_start_ts),
                second
            ) as gap_seconds,
            row_number() over (
                partition by job_id order by run_start_ts desc
            ) as recency,
        from runs
    )

select
    job_id,
    max(run_start_ts) as last_run_ts,
    approx_quantiles(if(recency <= 25, gap_seconds, null), 2 ignore nulls)[
        safe_offset(1)
    ] as typical_gap_seconds,
from gaps
group by job_id
"""


@dataclass(frozen=True)
class JobRun:
    """A job's latest run within the window, and how often it normally runs.

    Attributes:
        job_id: Orchestrator job (the Airflow DAG)
        last_run_ts: Start of its latest run
        typical_interval: Median gap between its recent runs, None with one run
    """

    job_id: str
    last_run_ts: datetime
    typical_interval: Optional[timedelta]

    def overdue_after(self) -> Optional[timedelta]:
        """How long after its latest run the job counts as not running any more.

        One and a half intervals plus 15 minutes, so a run that starts late or takes
        long doesn't trip it. None when the interval isn't known.
        """
        if self.typical_interval is None:
            return None
        return self.typical_interval * 1.5 + timedelta(minutes=15)

    def is_overdue(self, now: datetime) -> bool:
        limit = self.overdue_after()
        return limit is not None and now - self.last_run_ts > limit


@dataclass(frozen=True)
class Incident:
    """One node's unbroken streak of failed runs within one job.

    Attributes:
        job_id: Orchestrator job (the Airflow DAG) the runs belong to
        unique_id: dbt unique_id of the failing node
        name: Node name
        resource_type: model, snapshot, seed or test
        first_failed_ts: Start of the first failed run in the streak
        last_failed_ts: Start of the latest failed run
        failed_run_count: Number of failed runs in the streak
        is_start_truncated: True when no clean run precedes the streak inside the
            window, so it may have started earlier than first_failed_ts
        last_status: Status of the latest failed run: `fail` for a test that found
            rows, else `error` or `runtime error`
        last_message: Error message from the latest failed run
        last_failure_count: Rows the test returned in the latest failed run
        previous_failed_ts: Start of the failed run before the latest, if any
        cleared_ts: Start of the clean run that ended the streak, None while open
        is_failed_in_latest_run: The job's latest run is the latest failed run
        is_cleared_in_latest_run: The job's latest run is the one that cleared it
    """

    job_id: str
    unique_id: str
    name: str
    resource_type: str
    first_failed_ts: datetime
    last_failed_ts: datetime
    failed_run_count: int
    is_start_truncated: bool
    last_status: str
    last_message: Optional[str]
    last_failure_count: Optional[int]
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
        downstream: Models, snapshots and seeds below the failing nodes, including the
            node a failing test checks
        shared: The downstream nodes with `access` public or protected, i.e. the ones
            other groups build on
        exposures: Exposures below the failing nodes
    """

    downstream: Set[str] = field(default_factory=set)
    shared: Set[str] = field(default_factory=set)
    exposures: Set[str] = field(default_factory=set)


@dataclass
class ManifestGraph:
    """The parts of a manifest needed to work out a blast radius and name a cause.

    Attributes:
        tested: The nodes each data test checks
        test_kinds: The generic test each generic data test is an instance of
        exposure_types: Each exposure's dbt `type`, e.g. dashboard or application
        test_columns: The column each column-level data test checks
    """

    child_map: Dict[str, List[str]]
    access: Dict[str, str]
    names: Dict[str, str]
    tested: Dict[str, List[str]] = field(default_factory=dict)
    test_kinds: Dict[str, str] = field(default_factory=dict)
    exposure_types: Dict[str, str] = field(default_factory=dict)
    test_columns: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def load(cls, path: Path) -> "ManifestGraph":
        """Read the manifest; there is no blast radius without one, so it's required."""
        try:
            with open(path, "r", encoding="utf-8") as f:
                manifest = json.load(f)
        except (OSError, json.JSONDecodeError) as e:
            fatal(f"Could not read manifest {path}: {e}")
        nodes = manifest.get("nodes", {})
        sources = manifest.get("sources", {})
        exposures = manifest.get("exposures", {})
        tests = {
            uid: node
            for uid, node in nodes.items()
            if node.get("resource_type") == "test"
        }
        return cls(
            child_map=manifest.get("child_map", {}),
            access={uid: node.get("access") or "" for uid, node in nodes.items()},
            names={
                **{uid: node.get("name", uid) for uid, node in nodes.items()},
                **{uid: src.get("name", uid) for uid, src in sources.items()},
                **{
                    uid: exp.get("label") or exp.get("name", uid)
                    for uid, exp in exposures.items()
                },
            },
            # A generic test on a model is attached to it; a source or singular test
            # checks what it depends on.
            tested={
                uid: (
                    [node["attached_node"]]
                    if node.get("attached_node")
                    else node.get("depends_on", {}).get("nodes", [])
                )
                for uid, node in tests.items()
            },
            test_kinds={
                uid: ".".join(
                    part for part in (meta.get("namespace"), meta["name"]) if part
                )
                for uid, node in tests.items()
                if (meta := node.get("test_metadata") or {}).get("name")
            },
            exposure_types={
                uid: exp.get("type") or "unknown" for uid, exp in exposures.items()
            },
            test_columns={
                uid: node["column_name"]
                for uid, node in tests.items()
                if node.get("column_name")
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

    def at_risk(self, unique_id: str) -> Set[str]:
        """What a failing node affects: its descendants, or for a data test the nodes
        it checks and their descendants, since that is where the bad data is."""
        if unique_id not in self.tested:
            return self.descendants(unique_id)
        affected: Set[str] = set()
        for checked in self.tested[unique_id]:
            affected |= {checked} | self.descendants(checked)
        return affected

    def has_node(self, unique_id: str) -> bool:
        return unique_id in self.access

    def name(self, unique_id: str) -> str:
        return self.names.get(unique_id, unique_id.split(".")[-1])

    def sorted_names(self, unique_ids: Iterable[str]) -> List[str]:
        return sorted(self.name(uid) for uid in unique_ids)

    def test_kind(self, incident: "Incident") -> str:
        """The generic test a data test is an instance of; a singular test is its own."""
        return self.test_kinds.get(incident.unique_id, incident.name)

    def label(self, incident: "Incident") -> str:
        """How a failing node is shown: a data test as the node and column it checks,
        since its generated name is long and its cause already says which test it is.
        A test checking several nodes keeps its own name."""
        checked = self.tested.get(incident.unique_id, [])
        if incident.resource_type != "test" or len(checked) != 1:
            return incident.name
        column = self.test_columns.get(incident.unique_id)
        node = self.name(checked[0])
        return f"{node}.{column}" if column else node

    def exposures_by_type(self, unique_ids: Iterable[str]) -> Dict[str, List[str]]:
        """Exposure names grouped by their type, types in alphabetical order."""
        groups: Dict[str, List[str]] = {}
        for uid in unique_ids:
            kind = self.exposure_types.get(uid, "unknown")
            groups.setdefault(kind, []).append(uid)
        return {kind: self.sorted_names(groups[kind]) for kind in sorted(groups)}


def blast_radius(incidents: Iterable[Incident], graph: ManifestGraph) -> BlastRadius:
    """Everything below a set of failing nodes, other than the failing nodes themselves.

    This is the lineage, not what a run skipped: a run only selects some of what is
    downstream, and the rest goes stale just the same.
    """
    failing = {i.unique_id for i in incidents}
    below: Set[str] = set()
    for uid in failing:
        below |= graph.at_risk(uid)
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


_QUERY_URL = re.compile(r"\(Query: (https?://[^\s)]+)\)")


def query_link(message: Optional[str]) -> Optional[str]:
    """The console link to the failed BigQuery job, when the message carries one."""
    match = _QUERY_URL.search(message or "")
    return match.group(1) if match else None


def incident_cause(incident: Incident, graph: ManifestGraph) -> str:
    """What went wrong, worded so that incidents with one cause read the same.

    A data test that found rows has no message worth showing, so its cause is the
    generic test it is an instance of; a singular test is its own kind. A test that
    could not run has an error message like any other node.
    """
    if incident.last_status == "fail":
        return f"{graph.test_kind(incident)} test failed"
    return summarize_cause(incident.last_message)


def failure_detail(incident: Incident) -> str:
    """The latest failure in full: the error message, or the rows a test returned."""
    if incident.last_status == "fail" and incident.last_failure_count is not None:
        count = incident.last_failure_count
        return f"The test returned {count} failing row{'' if count == 1 else 's'}."
    return incident.last_message or ""


def node_count(incidents: List[Incident]) -> str:
    """How many models and tests are failing, e.g. `3 models, 30 tests`."""
    tests = sum(1 for i in incidents if i.resource_type == "test")
    counts = [(len(incidents) - tests, "model"), (tests, "test")]
    return ", ".join(f"{n} {word}{'' if n == 1 else 's'}" for n, word in counts if n)


def group_by_cause(
    incidents: Iterable[Incident], graph: ManifestGraph
) -> Dict[str, List[Incident]]:
    """Group incidents by their cause, longest-failing group first."""
    groups: Dict[str, List[Incident]] = {}
    for incident in incidents:
        groups.setdefault(incident_cause(incident, graph), []).append(incident)
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
            bq.ArrayQueryParameter(
                "failure_statuses", "STRING", list(FAILURE_STATUSES)
            ),
            bq.ArrayQueryParameter("resource_types", "STRING", list(RESOURCE_TYPES)),
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
            resource_type=r["resource_type"],
            first_failed_ts=r["first_failed_ts"],
            last_failed_ts=r["last_failed_ts"],
            failed_run_count=r["failed_run_count"],
            is_start_truncated=r["is_start_truncated"],
            last_status=r["last_status"],
            last_message=r["last_message"],
            last_failure_count=r["last_failure_count"],
            previous_failed_ts=r["previous_failed_ts"],
            cleared_ts=r["cleared_ts"],
            is_failed_in_latest_run=r["is_failed_in_latest_run"],
            is_cleared_in_latest_run=r["is_cleared_in_latest_run"],
        )
        for r in rows
    ]


def fetch_job_runs(as_of: datetime) -> List[JobRun]:
    """Each job's latest run and typical interval, in the window ending at as_of."""
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
        ]
    )
    rows = (
        client.get_client()
        .query(
            JOB_RUNS_SQL.format(dataset=dataset),
            job_config=job_config,
            location=GCP_LOCATION,
        )
        .result()
    )
    return [
        JobRun(
            job_id=r["job_id"],
            last_run_ts=r["last_run_ts"],
            typical_interval=(
                timedelta(seconds=r["typical_gap_seconds"])
                if r["typical_gap_seconds"]
                else None
            ),
        )
        for r in rows
    ]
