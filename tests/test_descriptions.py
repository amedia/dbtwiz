"""Tests for the node selection in sync-descriptions."""

from dbtwiz.admin.descriptions import _collect_nodes


def _node(name, tags=None, materialized="table", resource_type="model"):
    return {
        "resource_type": resource_type,
        "name": name,
        "database": "proj",
        "schema": "ds",
        "tags": tags or [],
        "config": {"materialized": materialized},
    }


def _manifest(*nodes):
    return {"nodes": {f"model.p.{n['name']}": n for n in nodes}}


MANIFEST = _manifest(
    _node("hourly_model", tags=["hourly"]),
    _node("daily_model", tags=["daily"]),
    _node("both_model", tags=["hourly", "daily"]),
    _node("untagged_model"),
    _node("ephemeral_model", tags=["hourly"], materialized="ephemeral"),
    _node("hourly_test", tags=["hourly"], resource_type="test"),
)


def test_collects_all_materialized_models_without_filters():
    assert set(_collect_nodes(MANIFEST, None)) == {
        "proj.ds.hourly_model",
        "proj.ds.daily_model",
        "proj.ds.both_model",
        "proj.ds.untagged_model",
    }


def test_tag_filter_keeps_nodes_with_the_tag():
    assert set(_collect_nodes(MANIFEST, None, ["hourly"])) == {
        "proj.ds.hourly_model",
        "proj.ds.both_model",
    }


def test_multiple_tags_select_the_union():
    assert set(_collect_nodes(MANIFEST, None, ["hourly", "daily"])) == {
        "proj.ds.hourly_model",
        "proj.ds.daily_model",
        "proj.ds.both_model",
    }


def test_tag_and_model_name_filters_combine():
    assert set(_collect_nodes(MANIFEST, ["both_model", "daily_model"], ["hourly"])) == {
        "proj.ds.both_model",
    }


def test_unknown_tag_selects_nothing():
    assert _collect_nodes(MANIFEST, None, ["nope"]) == {}
