"""Tests for reading dbt manifests, across both engines."""

import json

from dbtwiz.dbt.manifest import Manifest


def _write_manifest(
    tmp_path, sources=None, nodes=None, parent_map=None, child_map=None
):
    """Write a minimal manifest holding the given nodes, and return its path."""
    path = tmp_path / "manifest.json"
    path.write_text(
        json.dumps(
            {
                "nodes": nodes or {},
                "sources": sources or {},
                "parent_map": parent_map or {},
                "child_map": child_map or {},
            }
        ),
        encoding="utf-8",
    )
    return path


def _model_node(**overrides):
    """A model node as dbt emits one."""
    node = {
        "unique_id": "model.dbt_core.my_model",
        "resource_type": "model",
        "database": "my-project",
        "schema": "my_dataset",
        "name": "my_model",
        "alias": "my_model",
        "path": "3_intermediate/my_domain/my_model.sql",
        "tags": [],
        "meta": {},
        "description": "A model",
        "config": {"materialized": "table"},
    }
    node.update(overrides)
    return node


def _source_node(**overrides):
    """A source node as dbt v2 emits one — note the absence of `source_meta`."""
    node = {
        "unique_id": "source.dbt_core.my_source.my_table",
        "resource_type": "source",
        "database": "my-project",
        "schema": "my_dataset",
        "name": "my_table",
        "source_name": "my_source",
        "source_description": "A source",
        "identifier": "my_table_identifier",
        "path": "sources/my_source.yml",
        "description": "A table",
        "tags": [],
        "meta": {"owner": "#team-data"},
        "config": {"enabled": True},
    }
    node.update(overrides)
    return node


def test_sources_reads_v2_node_without_source_meta(tmp_path):
    """dbt v2 omits `source_meta`; source-level meta is inherited into `meta`."""
    path = _write_manifest(
        tmp_path, {"source.dbt_core.my_source.my_table": _source_node()}
    )

    sources = Manifest(path).sources()

    assert sources["my_table"]["source_meta"] == {}
    assert sources["my_table"]["meta"] == {"owner": "#team-data"}


def test_sources_keeps_source_meta_when_present(tmp_path):
    """dbt-core still emits it, and it must survive unchanged."""
    node = _source_node(source_meta={"owner": "#team-data"})
    path = _write_manifest(tmp_path, {"source.dbt_core.my_source.my_table": node})

    sources = Manifest(path).sources()

    assert sources["my_table"]["source_meta"] == {"owner": "#team-data"}


def test_table_reference_lookup_resolves_a_v2_source(tmp_path):
    """The lookup is what `model validate` uses to rewrite hardcoded table names."""
    path = _write_manifest(
        tmp_path, {"source.dbt_core.my_source.my_table": _source_node()}
    )

    lookup = Manifest(path).table_reference_lookup()

    assert lookup["my-project.my_dataset.my_table_identifier"] == (
        "source",
        ("my_source", "my_table"),
    )


def test_models_handles_a_model_without_a_description(tmp_path):
    """dbt does not require descriptions, so `deprecated` must not assume one."""
    node = _model_node()
    del node["description"]
    path = _write_manifest(
        tmp_path,
        nodes={"model.dbt_core.my_model": node},
        parent_map={"model.dbt_core.my_model": []},
        child_map={"model.dbt_core.my_model": []},
    )

    models = Manifest(path).models()

    assert models["my_model"]["deprecated"] is False


def test_models_marks_a_deprecated_model(tmp_path):
    """The description still drives the flag when it is there."""
    node = _model_node(description="DEPRECATED - use my_other_model instead")
    path = _write_manifest(
        tmp_path,
        nodes={"model.dbt_core.my_model": node},
        parent_map={"model.dbt_core.my_model": []},
        child_map={"model.dbt_core.my_model": []},
    )

    models = Manifest(path).models()

    assert models["my_model"]["deprecated"] is True


def test_models_handles_a_node_absent_from_parent_map(tmp_path):
    """child_models already guards this; parent_models must not diverge."""
    path = _write_manifest(
        tmp_path,
        nodes={"model.dbt_core.my_model": _model_node()},
        parent_map={},
        child_map={},
    )

    models = Manifest(path).models()

    assert models["my_model"]["parent_models"] == []
    assert models["my_model"]["child_models"] == []
