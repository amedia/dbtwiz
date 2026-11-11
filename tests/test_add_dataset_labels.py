"""Tests for the dataset selection, label merging and updates in add-dataset-labels."""

from types import SimpleNamespace

import pytest
import typer

from dbtwiz.admin.add_dataset_labels import (
    _apply_updates,
    _collect_datasets,
    _conflicting_labels,
    _missing_labels,
    _split_by_project,
)


def _node(name, schema, resource_type="model", materialized="table", database="proj"):
    return {
        "resource_type": resource_type,
        "name": name,
        "database": database,
        "schema": schema,
        "config": {"materialized": materialized},
    }


def _manifest(*nodes):
    return {"nodes": {f"{n['resource_type']}.p.{n['name']}": n for n in nodes}}


def test_collects_each_dataset_once():
    manifest = _manifest(_node("a", "ds"), _node("b", "ds"), _node("c", "other"))
    assert _collect_datasets(manifest) == {"proj.ds", "proj.other"}


def test_collects_seeds_and_snapshots():
    manifest = _manifest(
        _node("a_seed", "seeds", resource_type="seed", materialized="seed"),
        _node(
            "a_snapshot", "snapshots", resource_type="snapshot", materialized="snapshot"
        ),
    )
    assert _collect_datasets(manifest) == {"proj.seeds", "proj.snapshots"}


def test_skips_ephemeral_models_and_tests():
    manifest = _manifest(
        _node("eph", "eph_ds", materialized="ephemeral"),
        _node("a_test", "test_ds", resource_type="test", materialized="test"),
    )
    assert _collect_datasets(manifest) == set()


def test_keeps_the_project_apart():
    manifest = _manifest(_node("a", "ds"), _node("b", "ds", database="marts"))
    assert _collect_datasets(manifest) == {"proj.ds", "marts.ds"}


def test_missing_labels_on_unlabelled_dataset():
    assert _missing_labels({}, {"app": "dbt"}) == {"app": "dbt"}


def test_missing_labels_ignores_labels_already_set():
    assert _missing_labels({"app": "dbt", "team": "data"}, {"app": "dbt"}) == {}


def test_missing_labels_does_not_overwrite_another_value():
    assert _missing_labels({"app": "other"}, {"app": "dbt"}) == {}


def test_conflicting_labels_reports_another_value():
    assert _conflicting_labels({"app": "other"}, {"app": "dbt"}) == {"app": "other"}
    assert _conflicting_labels({"app": "dbt"}, {"app": "dbt"}) == {}


def test_split_by_project_keeps_datasets_in_the_projects():
    included, excluded = _split_by_project(
        {"core.b", "core.a", "marts.c", "other.ds"}, ["core", "marts"]
    )
    assert included == ["core.a", "core.b", "marts.c"]
    assert excluded == ["other.ds"]


def test_split_by_project_matches_the_whole_project_id():
    included, excluded = _split_by_project({"core-dev.a"}, ["core"])
    assert included == []
    assert excluded == ["core-dev.a"]


class _Forbidden(Exception):
    pass


class _FakeClient:
    """Records update_dataset calls; raises the given exception for the given dataset ids."""

    def __init__(self, raise_for=None):
        self.raise_for = raise_for or {}
        self.updated = []

    def update_dataset(self, dataset, fields):
        if dataset.dataset_id in self.raise_for:
            raise self.raise_for[dataset.dataset_id]
        self.updated.append((dataset.dataset_id, dict(dataset.labels), fields))


def _update(dataset_id, labels=None):
    dataset = SimpleNamespace(dataset_id=dataset_id, labels=labels or {})
    return {"dataset_id": dataset_id, "dataset": dataset, "missing": {"app": "dbt"}}


def test_apply_updates_adds_labels_and_keeps_the_others():
    client = _FakeClient()
    _apply_updates(client, [_update("p.a", labels={"team": "data"})], _Forbidden)
    assert client.updated == [("p.a", {"team": "data", "app": "dbt"}, ["labels"])]


def test_apply_updates_exits_on_a_permission_error():
    updates = [_update(f"p.d{i}") for i in range(100)]
    client = _FakeClient(raise_for={"p.d0": _Forbidden("denied")})
    with pytest.raises(typer.Exit):
        _apply_updates(client, updates, _Forbidden)


def test_apply_updates_exits_when_an_update_fails():
    client = _FakeClient(raise_for={"p.b": RuntimeError("boom")})
    with pytest.raises(typer.Exit):
        _apply_updates(client, [_update("p.a"), _update("p.b")], _Forbidden)
    assert [u[0] for u in client.updated] == ["p.a"]
