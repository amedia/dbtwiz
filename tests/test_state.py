"""Tests for detecting metadata-only changes against a state manifest."""

import copy
import json
from unittest.mock import patch

import pytest

from dbtwiz.dbt.state import (
    METADATA_CATEGORIES,
    detect_metadata_only_changes,
    mask_state,
)

MODEL_ID = "model.dbt_core.my_model"
SOURCE_ID = "source.dbt_core.my_source.my_table"


def _model(**overrides):
    """A model node with the fields dbt writes each config value to."""
    node = {
        "unique_id": MODEL_ID,
        "resource_type": "model",
        "description": "A model",
        "tags": ["daily"],
        "meta": {"teams": ["team-data"]},
        "config": {
            "materialized": "table",
            "tags": ["daily"],
            "meta": {"teams": ["team-data"]},
        },
        "unrendered_config": {"tags": ["daily"], "meta": {"teams": ["team-data"]}},
        "columns": {
            "id": {"name": "id", "description": "Id", "tags": [], "meta": {}},
        },
        "checksum": {"name": "sha256", "checksum": "abc"},
    }
    node.update(overrides)
    return node


def _source(**overrides):
    """A source table node with freshness configured."""
    freshness = {"warn_after": {"count": 30, "period": "hour"}}
    node = {
        "unique_id": SOURCE_ID,
        "resource_type": "source",
        "description": "A source",
        "freshness": freshness,
        "loaded_at_field": "loaded_at",
        "config": {"freshness": freshness, "loaded_at_field": "loaded_at"},
        "unrendered_config": {"freshness": freshness, "loaded_at_field": "loaded_at"},
        "columns": {},
    }
    node.update(overrides)
    return node


def _manifests(change_model=None, change_source=None):
    """A state manifest and a current one, the latter changed by the given functions."""
    state = {"nodes": {MODEL_ID: _model()}, "sources": {SOURCE_ID: _source()}}
    current = copy.deepcopy(state)
    if change_model:
        change_model(current["nodes"][MODEL_ID])
    if change_source:
        change_source(current["sources"][SOURCE_ID])
    return state, current


def _set_meta(node):
    for holder in (node, node["config"], node["unrendered_config"]):
        holder["meta"] = {"teams": ["team-data"], "access-policy": "generated-data"}


def _set_tags(node):
    for holder in (node, node["config"], node["unrendered_config"]):
        holder["tags"] = ["daily", "extra"]
    node["columns"]["id"]["tags"] = ["daily", "extra"]


def _set_freshness(node):
    freshness = {"warn_after": {"count": 31, "period": "hour"}}
    for holder in (node, node["config"], node["unrendered_config"]):
        holder["freshness"] = freshness


@pytest.mark.parametrize(
    "category, change_model, change_source",
    [
        ("description", lambda n: n.update(description="Changed"), None),
        (
            "description",
            lambda n: n["columns"]["id"].update(description="Changed"),
            None,
        ),
        ("tags", _set_tags, None),
        ("meta", _set_meta, None),
        ("freshness", None, _set_freshness),
        ("freshness", None, lambda n: n["config"].update(loaded_at_query="select 1")),
    ],
)
def test_masks_a_change_in_the_category(category, change_model, change_source):
    state, current = _manifests(change_model, change_source)

    masked = mask_state(state, current, [category])

    assert state == current
    changed_id = MODEL_ID if change_model else SOURCE_ID
    assert masked == {category: [changed_id]}


def test_leaves_a_change_outside_the_categories():
    def change(node):
        _set_meta(node)
        node["config"]["materialized"] = "view"
        node["checksum"]["checksum"] = "def"

    state, current = _manifests(change)

    mask_state(state, current, list(METADATA_CATEGORIES))

    assert state["nodes"][MODEL_ID]["meta"] == current["nodes"][MODEL_ID]["meta"]
    assert state["nodes"][MODEL_ID]["config"]["materialized"] == "table"
    assert state["nodes"][MODEL_ID]["checksum"]["checksum"] == "abc"


def test_masks_only_the_requested_categories():
    def change(node):
        _set_meta(node)
        node["description"] = "Changed"

    state, current = _manifests(change)

    masked = mask_state(state, current, ["description"])

    assert masked == {"description": [MODEL_ID]}
    assert state["nodes"][MODEL_ID]["description"] == "Changed"
    assert state["nodes"][MODEL_ID]["meta"] == {"teams": ["team-data"]}


def test_removes_a_value_the_current_node_no_longer_has():
    state, current = _manifests(lambda n: n["config"].pop("tags"))

    mask_state(state, current, ["tags"])

    assert "tags" not in state["nodes"][MODEL_ID]["config"]


def test_leaves_added_and_removed_columns():
    def change(node):
        node["columns"]["new"] = {"name": "new", "description": "New"}
        del node["columns"]["id"]

    state, current = _manifests(change)

    masked = mask_state(state, current, ["description"])

    assert masked == {"description": []}
    assert list(state["nodes"][MODEL_ID]["columns"]) == ["id"]


def test_leaves_added_and_removed_nodes():
    state, current = _manifests()
    del current["sources"][SOURCE_ID]
    current["nodes"]["model.dbt_core.new_model"] = _model(description="New")

    mask_state(state, current, list(METADATA_CATEGORIES))

    assert SOURCE_ID in state["sources"]
    assert "model.dbt_core.new_model" not in state["nodes"]


def _detect(tmp_path, state, current, categories):
    """Run the detection with dbt replaced by a node-by-node comparison."""
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    (state_dir / "manifest.json").write_text(json.dumps(state), encoding="utf-8")

    def fake_list_modified(state_dir, target_path, dbt_options):
        target_path.mkdir(parents=True)
        (target_path / "manifest.json").write_text(json.dumps(current), "utf-8")
        compared = json.loads((state_dir / "manifest.json").read_text("utf-8"))
        return sorted(
            unique_id
            for section in ("nodes", "sources")
            for unique_id, node in current[section].items()
            if compared[section].get(unique_id) != node
        )

    with patch("dbtwiz.dbt.state.list_modified", side_effect=fake_list_modified):
        result = detect_metadata_only_changes(state_dir, categories, {})
    # The state manifest itself is left alone
    assert json.loads((state_dir / "manifest.json").read_text("utf-8")) == state
    return result


def test_detects_metadata_only_changes(tmp_path):
    state, current = _manifests(_set_meta, _set_freshness)

    result = _detect(tmp_path, state, current, list(METADATA_CATEGORIES))

    assert result["metadata_only"] is True
    assert result["modified"] == [MODEL_ID, SOURCE_ID]
    assert result["modified_beyond_metadata"] == []


def test_detects_a_change_beyond_metadata(tmp_path):
    def change(node):
        _set_meta(node)
        node["checksum"]["checksum"] = "def"

    state, current = _manifests(change, _set_freshness)

    result = _detect(tmp_path, state, current, list(METADATA_CATEGORIES))

    assert result["metadata_only"] is False
    assert result["modified_beyond_metadata"] == [MODEL_ID]


def test_a_category_left_out_counts_as_a_change(tmp_path):
    state, current = _manifests(_set_meta)

    result = _detect(tmp_path, state, current, ["description", "tags"])

    assert result["metadata_only"] is False
    assert result["modified_beyond_metadata"] == [MODEL_ID]


def test_no_changes_is_not_metadata_only(tmp_path):
    state, current = _manifests()

    result = _detect(tmp_path, state, current, list(METADATA_CATEGORIES))

    assert result["metadata_only"] is False
    assert result["modified"] == []
