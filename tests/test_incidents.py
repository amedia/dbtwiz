"""Tests for incidents read from Elementary's run history, and the alert and report."""

import json
from datetime import datetime, timedelta, timezone

import pytest

from dbtwiz.admin.incident_alert import (
    _TransientError,
    crossed_threshold,
    post_to_slack,
    resolve_mentions,
    should_announce_recovery,
    should_escalate,
    slack_message,
    validate_mentions,
)
from dbtwiz.admin.incident_report import (
    ReportLinks,
    render_report,
    status_summary,
    summary_destination,
)
from dbtwiz.admin.incidents import (
    Incident,
    JobRun,
    ManifestGraph,
    blast_radius,
    drop_removed_nodes,
    format_duration,
    group_by_cause,
    node_count,
    query_link,
    summarize_cause,
)

BASE = timedelta(hours=3)
T0 = datetime(2026, 10, 13, 15, 0, tzinfo=timezone.utc)

MISSING_SOURCE = (
    "Database Error in model int_user__relations "
    "(target/run/dbt_core/models/int_user__relations.sql)\n"
    "  [BigQuery] googleapi: Error 404: Not found: Table "
    "amedia-adp-sources:dlt_database.asp__customer_state_historized was not found"
)


def incident(
    hours_failing: float,
    previous_hours: float | None,
    *,
    job_id="adp__hourly",
    unique_id="model.p.int_user__relations",
    cleared_after_hours: float | None = None,
    failed_in_latest=True,
    cleared_in_latest=False,
    message=MISSING_SOURCE,
    failures: int | None = None,
) -> Incident:
    """A failing node; with `failures`, a data test that returned that many rows."""
    return Incident(
        job_id=job_id,
        unique_id=unique_id,
        name=unique_id.split(".")[2],
        resource_type=unique_id.split(".")[0],
        first_failed_ts=T0,
        last_failed_ts=T0 + timedelta(hours=hours_failing),
        failed_run_count=int(hours_failing) + 1,
        is_start_truncated=False,
        last_status="error" if failures is None else "fail",
        last_message=message if failures is None else "Error",
        last_failure_count=failures,
        previous_failed_ts=(
            T0 + timedelta(hours=previous_hours) if previous_hours is not None else None
        ),
        cleared_ts=(
            T0 + timedelta(hours=cleared_after_hours)
            if cleared_after_hours is not None
            else None
        ),
        is_failed_in_latest_run=failed_in_latest,
        is_cleared_in_latest_run=cleared_in_latest,
    )


GRAPH = ManifestGraph(
    child_map={
        "model.p.int_user__relations": [
            "model.p.int_user__a",
            "model.p.mrt_user__b",
            "test.p.not_null_relations.3",
        ],
        "model.p.int_user__a": ["model.p.mrt_user__c"],
        "model.p.mrt_user__b": ["exposure.p.looker_users"],
        "model.p.mrt_user__c": [],
        "model.p.int_other": ["model.p.mrt_other"],
        "model.p.mrt_other": [],
        "source.p.src.a": ["model.p.int_other", "test.p.gaps_src_a.1"],
        "source.p.src.b": ["model.p.int_other", "test.p.gaps_src_b.2"],
    },
    access={
        "model.p.int_user__relations": "private",
        "model.p.int_user__a": "private",
        "model.p.mrt_user__b": "public",
        "model.p.mrt_user__c": "protected",
        "model.p.int_other": "private",
        "model.p.mrt_other": "public",
    },
    names={"exposure.p.looker_users": "Looker: users"},
    tested={
        "test.p.gaps_src_a.1": ["source.p.src.a"],
        "test.p.gaps_src_b.2": ["source.p.src.b"],
        "test.p.not_null_relations.3": ["model.p.int_user__relations"],
    },
    test_kinds={
        "test.p.gaps_src_a.1": "partition_gaps",
        "test.p.gaps_src_b.2": "partition_gaps",
        "test.p.not_null_relations.3": "not_null",
    },
    exposure_types={"exposure.p.looker_users": "dashboard"},
)


@pytest.mark.parametrize(
    "previous,current,expected",
    [
        # Hourly: 3h, 6h, 12h, 24h each fire once, at the run that passes them.
        (2, 3, 3),
        (3, 4, None),
        (5, 6, 6),
        (11, 12, 12),
        (23, 24, 24),
        (24, 25, None),
        # Daily: the second failure passes 3h, 6h and 12h at once and posts once.
        (0, 23.9, 12),
        (23.9, 47.9, 24),
        # The first failed run has no previous run and never escalates.
        (None, 0, None),
    ],
)
def test_crossed_threshold(previous, current, expected):
    """Thresholds double from the base and each fires at the run that passes it."""
    result = crossed_threshold(
        timedelta(hours=previous) if previous is not None else None,
        timedelta(hours=current),
        BASE,
    )
    assert result == (timedelta(hours=expected) if expected is not None else None)


def test_escalation_tolerates_run_start_jitter():
    """The run three hours after the first failure often starts at 2h59m."""
    assert should_escalate(incident(2 + 59 / 60, 1 + 59 / 60), BASE)
    assert not should_escalate(incident(3 + 59 / 60, 2 + 59 / 60), BASE)


def test_escalation_needs_the_latest_run_to_have_failed():
    """A node not selected in the job's latest run is still open but posts nothing."""
    assert should_escalate(incident(3, 2), BASE)
    assert not should_escalate(incident(3, 2, failed_in_latest=False), BASE)
    assert not should_escalate(incident(3, 2, cleared_after_hours=4), BASE)


def test_recovery_is_announced_only_for_escalated_incidents():
    """A blip that never reached the first threshold clears silently."""
    long = incident(14, 13, cleared_after_hours=15, cleared_in_latest=True)
    blip = incident(1, 0, cleared_after_hours=2, cleared_in_latest=True)
    assert should_announce_recovery(long, BASE)
    assert not should_announce_recovery(blip, BASE)


def test_summarize_cause_drops_the_node_specific_line():
    assert summarize_cause(MISSING_SOURCE) == (
        "Error 404: Not found: Table "
        "amedia-adp-sources:dlt_database.asp__customer_state_historized was not found"
    )
    assert summarize_cause(
        MISSING_SOURCE
        + ", notFound (Query: https://console.cloud.google.com/bigquery?j=bq:EU:a&b=1)"
    ).endswith("was not found, notFound")
    assert summarize_cause(None) == "No error message"
    assert summarize_cause("x" * 400).endswith("…")


def test_group_by_cause_merges_failures_with_one_cause():
    """Two models failing on the same missing table are one root cause."""
    other = MISSING_SOURCE.replace("int_user__relations", "int_user__other")
    groups = group_by_cause(
        [
            incident(5, 4),
            incident(14, 13, unique_id="model.p.int_user__other", message=other),
            incident(3, 2, unique_id="model.p.x", message="Runtime Error in x\n  boom"),
        ],
        GRAPH,
    )
    causes = list(groups)
    assert len(causes) == 2
    assert [i.name for i in groups[causes[0]]] == [
        "int_user__other",
        "int_user__relations",
    ]
    assert causes[1] == "boom"


def test_failing_tests_group_by_the_generic_test():
    """A failing test's message says nothing; the test that failed is the cause."""
    gaps_a = incident(6, 5, unique_id="test.p.gaps_src_a.1", failures=12)
    gaps_b = incident(6, 5, unique_id="test.p.gaps_src_b.2", failures=1)
    singular = incident(6, 5, unique_id="test.p.assert_x.4", failures=12)
    broken = incident(
        6, 5, unique_id="test.p.not_null_relations.3", message=MISSING_SOURCE
    )
    groups = group_by_cause([gaps_a, gaps_b, singular, broken], GRAPH)
    assert groups["partition_gaps test failed"] == [gaps_a, gaps_b]
    assert groups["assert_x test failed"] == [singular]
    # A test that could not run is grouped by its error, like a model.
    assert broken in groups[summarize_cause(MISSING_SOURCE)]


def test_blast_radius_is_everything_downstream():
    """The whole lineage below the failure counts, tests excluded, unrelated nodes not."""
    radius = blast_radius([incident(3, 2)], GRAPH)
    assert radius.downstream == {
        "model.p.int_user__a",
        "model.p.mrt_user__b",
        "model.p.mrt_user__c",
    }
    assert radius.shared == {"model.p.mrt_user__b", "model.p.mrt_user__c"}
    assert radius.exposures == {"exposure.p.looker_users"}


def test_blast_radius_excludes_the_failing_nodes_themselves():
    """A failing model below another failing one is a failure, not collateral."""
    radius = blast_radius(
        [incident(3, 2), incident(3, 2, unique_id="model.p.int_user__a")], GRAPH
    )
    assert "model.p.int_user__a" not in radius.downstream


def test_blast_radius_of_a_failing_test_starts_at_what_it_checks():
    """A model test puts the model at risk; a source test what is built on the source."""
    on_model = blast_radius(
        [incident(3, 2, unique_id="test.p.not_null_relations.3")], GRAPH
    )
    assert "model.p.int_user__relations" in on_model.downstream
    assert "model.p.mrt_user__c" in on_model.downstream
    on_source = blast_radius([incident(3, 2, unique_id="test.p.gaps_src_a.1")], GRAPH)
    assert on_source.downstream == {"model.p.int_other", "model.p.mrt_other"}
    assert on_source.shared == {"model.p.mrt_other"}


def test_manifest_graph_reads_what_each_test_checks(tmp_path):
    manifest = {
        "nodes": {
            "model.p.m": {"name": "m", "resource_type": "model"},
            "test.p.not_null_m_id.1": {
                "name": "not_null_m_id",
                "resource_type": "test",
                "attached_node": "model.p.m",
                "depends_on": {"nodes": ["model.p.m"]},
                "test_metadata": {"name": "not_null", "namespace": None},
            },
            "test.p.gaps_src_a.2": {
                "name": "gaps_src_a",
                "resource_type": "test",
                "attached_node": None,
                "depends_on": {"nodes": ["source.p.src.a"]},
                "test_metadata": {"name": "partition_gaps", "namespace": "pkg"},
            },
            "test.p.assert_x.3": {
                "name": "assert_x",
                "resource_type": "test",
                "depends_on": {"nodes": ["model.p.m"]},
            },
        },
        "exposures": {
            "exposure.p.sales": {"name": "sales", "type": "dashboard"},
        },
        "child_map": {},
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    graph = ManifestGraph.load(path)
    assert graph.tested == {
        "test.p.not_null_m_id.1": ["model.p.m"],
        "test.p.gaps_src_a.2": ["source.p.src.a"],
        "test.p.assert_x.3": ["model.p.m"],
    }
    assert graph.test_kinds == {
        "test.p.not_null_m_id.1": "not_null",
        "test.p.gaps_src_a.2": "pkg.partition_gaps",
    }
    assert graph.exposure_types == {"exposure.p.sales": "dashboard"}


def test_open_incidents_of_removed_nodes_are_dropped():
    """A deleted model never runs again, so its incident would never clear."""
    removed_open = incident(3, 2, unique_id="model.p.gone")
    removed_cleared = incident(3, 2, unique_id="model.p.gone2", cleared_after_hours=4)
    kept = incident(3, 2, unique_id="model.p.mrt_user__b")
    result = drop_removed_nodes([removed_open, removed_cleared, kept], GRAPH)
    assert result == [removed_cleared, kept]


def test_slack_message_for_an_escalation_and_a_recovery():
    message = slack_message(
        "adp__hourly",
        [
            incident(6, 5),
            incident(
                14,
                13,
                unique_id="model.p.int_other",
                cleared_after_hours=15,
                failed_in_latest=False,
                cleared_in_latest=True,
            ),
            # Another job's incident is that job's to post.
            incident(6, 5, job_id="adp__daily", unique_id="model.p.int_daily"),
        ],
        BASE,
        GRAPH,
        timezone.utc,
        "https://example.com/incidents",
    )
    assert message is not None
    assert "asp__customer_state_historized" in message
    assert (
        "`int_user__relations` failing for 6h (7 runs, since 2026-10-13 15:00)"
        in message
    )
    assert "3 downstream models, 2 of them public/protected" in message
    assert "Downstream dashboard exposures: `Looker: users`" in message
    assert "`int_other` recovered after 15h" in message
    assert "int_daily" not in message
    assert message.endswith("<https://example.com/incidents|Incident report>")


def test_failing_tests_escalate_as_one_section_per_generic_test():
    message = slack_message(
        "adp__daily",
        [
            incident(
                24,
                0,
                job_id="adp__daily",
                unique_id=f"test.p.gaps_src_{s}.{n}",
                failures=12,
            )
            for n, s in ((1, "a"), (2, "b"))
        ],
        BASE,
        GRAPH,
        timezone.utc,
        None,
    )
    assert message is not None
    assert message.count(":rotating_light:") == 1
    assert "*adp__daily*: partition_gaps test failed" in message
    assert "`gaps_src_a` failing for 1d" in message
    assert "2 downstream models, 1 of them public/protected" in message
    # The blast radius is counted, not sampled by name.
    assert "mrt_other" not in message


@pytest.mark.parametrize(
    "exports,expected",
    [
        (
            6,
            "Downstream application exposures: `export_0`, `export_1`, `export_2`,"
            " `export_3`, `export_4`, `export_5`\n"
            "Downstream dashboard exposures: `dash_0`, `dash_1`",
        ),
        # The limit is on all exposures, not on each type.
        (7, "Exposures downstream by type: application 7, dashboard 2"),
    ],
)
def test_exposures_are_named_by_type_or_counted_past_the_limit(exports, expected):
    types = {
        **{f"exposure.p.export_{n}": "application" for n in range(exports)},
        **{f"exposure.p.dash_{n}": "dashboard" for n in range(2)},
    }
    graph = ManifestGraph(
        child_map={"model.p.src": list(types)},
        access={},
        names={},
        exposure_types=types,
    )
    message = slack_message(
        "adp__hourly",
        [incident(6, 5, unique_id="model.p.src")],
        BASE,
        graph,
        timezone.utc,
        None,
    )
    assert message is not None
    assert message.endswith(expected)


def test_a_large_escalation_gives_a_count_not_names():
    message = slack_message(
        "adp__hourly",
        [incident(6, 5, unique_id=f"model.p.m{n}") for n in range(6)],
        BASE,
        GRAPH,
        timezone.utc,
        None,
    )
    assert message is not None
    assert "6 models failing for up to 6h (since 2026-10-13 15:00)" in message
    assert "`m0`" not in message


def test_no_slack_message_between_thresholds():
    assert (
        slack_message("adp__hourly", [incident(4, 3)], BASE, GRAPH, timezone.utc, None)
        is None
    )


@pytest.mark.parametrize(
    "duration,expected",
    [
        (timedelta(minutes=45), "45m"),
        (timedelta(hours=1, minutes=30), "1h 30m"),
        (timedelta(hours=2, minutes=59), "3h"),
        (timedelta(hours=5, minutes=59), "6h"),
        (timedelta(hours=14, minutes=5), "14h"),
        (timedelta(days=2, hours=3), "2d 3h"),
        (timedelta(days=2), "2d"),
    ],
)
def test_format_duration(duration, expected):
    assert format_duration(duration) == expected


def test_render_report_lists_open_and_cleared_incidents_escaped():
    html = render_report(
        [
            incident(14, 13),
            incident(
                5,
                4,
                unique_id="model.p.int_other",
                cleared_after_hours=6,
                message="Runtime Error in x\n  <script>",
            ),
            incident(2, 1, unique_id="test.p.gaps_src_a.1", failures=12),
        ],
        [],
        GRAPH,
        timezone.utc,
        T0 + timedelta(hours=14),
        30,
    )
    assert "int_user__relations" in html
    assert "3 downstream</span>, 2 public/protected, 1 exposures" in html
    assert "Exposures: dashboard" in html
    # Each downstream model is listed once: public/protected, or private and other.
    assert html.count("<code>mrt_user__b</code>") == 1
    assert "<code>int_user__a</code>" in html
    assert 'class="sev-high"' in html  # 14h open
    assert 'class="cleared-row"' in html
    assert "int_other" in html
    assert '<code>gaps_src_a</code> <span class="kind">test</span>' in html
    assert "partition_gaps test failed" in html
    assert "The test returned 12 failing rows." in html
    # The error message is escaped, not injected into the page.
    assert "&lt;script&gt;" in html
    assert "Runtime Error in x\n  <script>" not in html


HOURLY = JobRun("adp__hourly", T0 + timedelta(hours=14), timedelta(hours=1))


def test_a_job_is_overdue_after_one_and_a_half_intervals_and_a_margin():
    """An hourly job is late after 1h45m, not at its next scheduled minute."""
    last = HOURLY.last_run_ts
    assert not HOURLY.is_overdue(last + timedelta(minutes=104))
    assert HOURLY.is_overdue(last + timedelta(minutes=106))
    assert not JobRun("adp__once", last, None).is_overdue(last + timedelta(days=9))


def test_links_follow_dbt_docs_and_airflow_routes():
    links = ReportLinks(
        dbt_docs_url="https://docs/adp-dbt-core/index.html",
        airflow_url="https://airflow/",
    )
    assert links.model("model.p.int_x") == (
        "https://docs/adp-dbt-core/index.html#!/model/model.p.int_x"
    )
    assert links.model("snapshot.p.snp_x").endswith("#!/snapshot/snapshot.p.snp_x")
    assert links.job("adp__hourly") == "https://airflow/dags/adp__hourly"
    assert ReportLinks().model("model.p.int_x") is None


def test_query_link_is_taken_from_the_error_message():
    url = "https://console.cloud.google.com/bigquery?project=p&j=bq:EU:a"
    assert query_link(f"{MISSING_SOURCE}, notFound (Query: {url})") == url
    assert query_link(MISSING_SOURCE) is None


def test_report_has_one_open_row_per_job_and_cause():
    """Thirty tests failing on one cause are one row listing them, not thirty rows."""
    html = render_report(
        [
            incident(6, 5, unique_id="test.p.gaps_src_a.1", failures=1),
            incident(5, 4, unique_id="test.p.gaps_src_b.2", failures=3),
            incident(3, 2),
        ],
        [],
        GRAPH,
        timezone.utc,
        T0 + timedelta(hours=6),
        30,
    )
    assert html.count('<tr class="sev-') == 2
    assert "<summary>2 tests</summary>" in html
    assert '<span class="status bad">3 open</span>' in html
    # The combined blast radius of both source tests.
    assert "2 downstream</span>, 1 public/protected" in html


@pytest.mark.parametrize(
    "unique_ids,expected",
    [
        (["model.p.a"], "1 model"),
        (["test.p.a.1", "test.p.b.2"], "2 tests"),
        (["model.p.a", "seed.p.b", "test.p.c.1"], "2 models, 1 test"),
    ],
)
def test_node_count(unique_ids, expected):
    assert node_count([incident(3, 2, unique_id=u) for u in unique_ids]) == expected


def test_report_shows_job_status_and_links():
    links = ReportLinks(
        dbt_docs_url="https://docs/index.html", airflow_url="https://af"
    )
    html = render_report(
        [incident(14, 13)],
        [HOURLY, JobRun("adp__daily", T0, timedelta(days=1))],
        GRAPH,
        timezone.utc,
        T0 + timedelta(hours=14, minutes=10),
        30,
        90,
        links,
    )
    assert 'data-job="adp__daily"' in html
    assert "Runs every 1d" in html
    assert "1 open" in html  # the hourly job's card
    assert "https://docs/index.html#!/model/model.p.int_user__relations" in html
    assert "https://af/dags/adp__hourly" in html
    assert "5400 * 1000" in html  # 90 minutes before the page calls itself stale


def test_status_summary_for_an_overview_page():
    summary = status_summary(
        [
            incident(14, 13),
            incident(5, 4, unique_id="model.p.x", cleared_after_hours=6),
            incident(15, 14, unique_id="test.p.gaps_src_a.1", failures=12),
        ],
        [HOURLY],
        GRAPH,
        T0 + timedelta(hours=14, minutes=10),
        30,
        "incident_report.html",
        1560,
    )
    assert summary["schema_version"] == 1
    assert summary["stale_after_minutes"] == 1560
    assert summary["report_file"] == "incident_report.html"
    assert summary["open_incident_count"] == 2
    assert summary["cleared_incident_count"] == 1
    assert summary["jobs"] == [
        {
            "job_id": "adp__hourly",
            "last_attempt_at": "2026-10-14T05:00:00+00:00",
            "usual_gap_minutes": 60,
            # 1.5 hourly gaps plus 15 minutes after the last attempt
            "overdue_at": "2026-10-14T06:45:00+00:00",
            "is_overdue": False,
            "open_incident_count": 2,
        }
    ]
    first, test = summary["open_incidents"]
    assert first["unique_id"] == "model.p.int_user__relations"
    assert first["name"] == "int_user__relations"
    assert first["resource_type"] == "model"
    assert first["failing_since"] == "2026-10-13T15:00:00+00:00"
    assert test["resource_type"] == "test"
    assert test["cause"] == "partition_gaps test failed"
    assert json.dumps(summary)  # plain data only


@pytest.mark.parametrize(
    "destination,expected",
    [
        (
            "gs://b/adp-dbt-core/incident_report.html",
            "gs://b/adp-dbt-core/incident_report.json",
        ),
        ("report", "report.json"),
    ],
)
def test_summary_goes_beside_the_page(destination, expected):
    assert summary_destination(destination) == expected


class FakeSlack:
    """Answers Slack API calls from a script of replies per method, and records them."""

    def __init__(self, **replies):
        self.replies = {method: list(r) for method, r in replies.items()}
        self.calls = []

    def __call__(self, method, token, params):
        self.calls.append((method, dict(params)))
        reply = self.replies[method].pop(0)
        if isinstance(reply, Exception):
            raise reply
        return reply


def _post(fake):
    sleeps = []
    post_to_slack("xoxb", "adp-incidents", "hi", api=fake, sleep=sleeps.append)
    return sleeps


def test_post_succeeds_first_time():
    fake = FakeSlack(**{"chat.postMessage": [{"ok": True}]})
    assert _post(fake) == []
    assert fake.calls == [
        (
            "chat.postMessage",
            {"channel": "adp-incidents", "text": "hi", "unfurl_links": "false"},
        )
    ]


def test_transient_failures_are_retried_with_backoff():
    fake = FakeSlack(
        **{
            "chat.postMessage": [
                _TransientError("HTTP 503"),
                {"ok": False, "error": "ratelimited"},
                {"ok": True},
            ]
        }
    )
    assert _post(fake) == [1, 2]


def test_gives_up_after_the_last_attempt():
    fake = FakeSlack(**{"chat.postMessage": [_TransientError("HTTP 503")] * 3})
    with pytest.raises(RuntimeError, match="after 3 attempts: HTTP 503"):
        _post(fake)


def test_joins_the_channel_by_id_when_not_in_it():
    """conversations.join takes an ID, so a channel given by name is looked up."""
    fake = FakeSlack(
        **{
            "chat.postMessage": [
                {"ok": False, "error": "not_in_channel"},
                {"ok": True},
            ],
            "conversations.list": [
                {
                    "ok": True,
                    "channels": [{"name": "other", "id": "C0OTHER01"}],
                    "response_metadata": {"next_cursor": "page2"},
                },
                {
                    "ok": True,
                    "channels": [{"name": "adp-incidents", "id": "C0INCID01"}],
                },
            ],
            "conversations.join": [{"ok": True}],
        }
    )
    assert _post(fake) == []
    assert fake.calls[2][1]["cursor"] == "page2"
    assert fake.calls[3] == ("conversations.join", {"channel": "C0INCID01"})


def test_a_channel_it_cannot_join_says_to_invite_the_app():
    """A private channel isn't in the public list, so only an invite helps."""
    fake = FakeSlack(
        **{
            "chat.postMessage": [{"ok": False, "error": "not_in_channel"}],
            "conversations.list": [{"ok": True, "channels": []}],
        }
    )
    with pytest.raises(RuntimeError, match="Invite it to the channel"):
        _post(fake)


@pytest.mark.parametrize(
    "error,match",
    [
        ("channel_not_found", "was not found"),
        ("invalid_auth", "rejected the token"),
        ("msg_too_long", "rejected the message: msg_too_long"),
    ],
)
def test_errors_no_retry_can_fix_raise_at_once(error, match):
    fake = FakeSlack(**{"chat.postMessage": [{"ok": False, "error": error}]})
    with pytest.raises(RuntimeError, match=match):
        _post(fake)
    assert len(fake.calls) == 1


def test_mentions_are_added_to_escalations_only():
    mentions = ["<@U012ABCDEF>", "<!subteam^S012ABCDEF>"]
    escalation = slack_message(
        "adp__hourly", [incident(6, 5)], BASE, GRAPH, timezone.utc, None, mentions
    )
    assert "<@U012ABCDEF> <!subteam^S012ABCDEF>" in escalation
    recovered = incident(
        14, 13, cleared_after_hours=15, failed_in_latest=False, cleared_in_latest=True
    )
    recovery = slack_message(
        "adp__hourly", [recovered], BASE, GRAPH, timezone.utc, None, mentions
    )
    assert "<@" not in recovery


def test_mentions_are_handles_or_slack_tokens():
    """Anything else would post as plain text and notify nobody."""
    valid = ["@firstname.lastname", "@data-team", "@here", "<@U012ABCDEF>"]
    assert validate_mentions(valid) == valid
    with pytest.raises(ValueError, match="firstname, U012ABCDEF"):
        validate_mentions(["@firstname.lastname", "firstname", "U012ABCDEF"])


SLACK_DIRECTORY = {
    "users.list": [
        {
            "ok": True,
            "members": [
                {"id": "U0OLD0001", "name": "firstname.lastname", "deleted": True},
                {
                    "id": "U0PERSON1",
                    "name": "firstname.lastname",
                    "profile": {"display_name": "Firstname"},
                },
            ],
            "response_metadata": {"next_cursor": "page2"},
        },
        {
            "ok": True,
            "members": [
                {
                    "id": "U0PERSON2",
                    "name": "another.person",
                    "profile": {"display_name": ""},
                }
            ],
        },
    ],
    "usergroups.list": [
        {"ok": True, "usergroups": [{"id": "S0DATA001", "handle": "data-team"}]}
    ],
}


def test_handles_resolve_to_mention_tokens():
    """People by username or display name, groups by handle, any case."""
    fake = FakeSlack(**SLACK_DIRECTORY)
    resolved = resolve_mentions(
        ["@Firstname", "@another.person", "@data-team", "@here", "<@U0RAW0001>"],
        "xoxb",
        fake,
    )
    assert resolved == [
        "<@U0PERSON1>",
        "<@U0PERSON2>",
        "<!subteam^S0DATA001>",
        "<!here>",
        "<@U0RAW0001>",
    ]


def test_no_lookup_when_nothing_needs_resolving():
    """IDs written like handles are wrapped, not looked up by name."""
    fake = FakeSlack()
    assert resolve_mentions(
        ["@here", "<@U0RAW0001>", "@U111D1DB111", "@S012ABCDEF"], "xoxb", fake
    ) == ["<!here>", "<@U0RAW0001>", "<@U111D1DB111>", "<!subteam^S012ABCDEF>"]
    assert fake.calls == []


def test_an_unknown_handle_still_lets_the_alert_through():
    """A typo'd handle is posted as text with a note, never dropping the alert."""
    fake = FakeSlack(**SLACK_DIRECTORY)
    assert resolve_mentions(["@firstname.lastname", "@nobody"], "xoxb", fake) == [
        "<@U0PERSON1>",
        "@nobody (not found in Slack)",
    ]


def test_a_failed_lookup_still_lets_the_alert_through():
    fake = FakeSlack(**{"users.list": [{"ok": False, "error": "missing_scope"}]})
    assert resolve_mentions(["@firstname.lastname"], "xoxb", fake) == [
        "@firstname.lastname (not found in Slack)"
    ]
