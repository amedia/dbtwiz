"""Detect when every change against a state manifest is metadata only."""

import json
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from ..utils.logger import debug
from .run import dbt_args, dbt_executable

# Paths within a node or source that each category covers. "*" under "columns" stands
# for every column the node has in both manifests. dbt writes each config value three
# times: on the node, under `config` and under `unrendered_config`.
METADATA_CATEGORIES: Dict[str, List[Tuple[str, ...]]] = {
    "description": [
        ("description",),
        ("columns", "*", "description"),
    ],
    "tags": [
        ("tags",),
        ("config", "tags"),
        ("unrendered_config", "tags"),
        ("columns", "*", "tags"),
    ],
    "meta": [
        ("meta",),
        ("config", "meta"),
        ("unrendered_config", "meta"),
        ("columns", "*", "meta"),
    ],
    "freshness": [
        (*section, *path)
        for section in ((), ("config",), ("unrendered_config",))
        for path in (("freshness",), ("loaded_at_field",), ("loaded_at_query",))
    ],
}

_MISSING = object()


def _get(node: Dict[str, Any], path: Tuple[str, ...]) -> Any:
    value: Any = node
    for key in path:
        if not isinstance(value, dict) or key not in value:
            return _MISSING
        value = value[key]
    return value


def _copy(
    state: Dict[str, Any], current: Dict[str, Any], path: Tuple[str, ...]
) -> bool:
    """Set `path` in the state node to its value in the current node.

    Returns True when the two differed. A path missing from the current node is removed
    from the state node; a parent missing from the state node is left alone, since that
    is a structural change rather than a metadata one.
    """
    new = _get(current, path)
    if _get(state, path) == new:
        return False
    parent = _get(state, path[:-1]) if len(path) > 1 else state
    if not isinstance(parent, dict):
        return False
    if new is _MISSING:
        parent.pop(path[-1], None)
    else:
        parent[path[-1]] = new
    return True


def _expand(
    state: Dict[str, Any], current: Dict[str, Any], path: Tuple[str, ...]
) -> Iterable[Tuple[str, ...]]:
    """Expand a column wildcard into one path per column present in both nodes."""
    if "*" not in path:
        yield path
        return
    star = path.index("*")
    state_columns = _get(state, path[:star])
    current_columns = _get(current, path[:star])
    if not isinstance(state_columns, dict) or not isinstance(current_columns, dict):
        return
    for column in state_columns.keys() & current_columns.keys():
        yield path[:star] + (column,) + path[star + 1 :]


def mask_state(
    state: Dict[str, Any], current: Dict[str, Any], categories: Iterable[str]
) -> Dict[str, List[str]]:
    """Copy the given metadata categories from the current manifest into the state one.

    Nodes and sources present in only one of the manifests are left alone, so dbt still
    sees them as added or removed. Everything outside the categories is untouched, so
    dbt's own comparison decides what else counts as modified.

    Returns:
        For each category, the unique ids of the nodes whose metadata differed
    """
    masked: Dict[str, List[str]] = {}
    for category in categories:
        paths = METADATA_CATEGORIES[category]
        changed = set()
        for section in ("nodes", "sources"):
            current_nodes = current.get(section, {})
            for unique_id, state_node in state.get(section, {}).items():
                current_node = current_nodes.get(unique_id)
                if current_node is None:
                    continue
                for path in paths:
                    for concrete in _expand(state_node, current_node, path):
                        if _copy(state_node, current_node, concrete):
                            changed.add(unique_id)
        masked[category] = sorted(changed)
    return masked


def list_modified(
    state_dir: Path, target_path: Path, dbt_options: Dict[str, str]
) -> List[str]:
    """The unique ids `dbt ls -s state:modified` selects against the given state.

    dbt writes the current project's manifest to `target_path` as it lists.
    """
    command = [dbt_executable()] + dbt_args(
        ["ls"],
        # --quiet keeps the banner, warnings and summary off stdout, so what is
        # captured below is nothing but the JSON lines.
        quiet=True,
        select="state:modified",
        state=str(state_dir),
        output="json",
        output_keys="unique_id",
        target_path=str(target_path),
        **dbt_options,
    )
    debug(f"Invoking dbt with args: {command[1:]}")
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"dbt ls failed with exit code {result.returncode}:\n"
            f"{result.stderr or result.stdout}"
        )
    return sorted(
        json.loads(line)["unique_id"]
        for line in result.stdout.splitlines()
        if line.strip().startswith("{")
    )


def detect_metadata_only_changes(
    state_dir: Path, categories: Iterable[str], dbt_options: Dict[str, str]
) -> Dict[str, Any]:
    """Find out whether every change against the state is in the given categories.

    Lists `state:modified` twice: against the state, and against a temporary copy of
    it with the categories taken from the current manifest. dbt itself decides what
    counts as modified both times; the copy is removed afterwards and used for nothing
    else.

    Returns:
        `metadata_only`, true when something is modified and nothing is left once the
        categories are masked; `modified`, the nodes dbt selects against the state;
        and `modified_beyond_metadata`, those it still selects once they are masked
    """
    categories = list(categories)
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        modified = list_modified(state_dir, tmp_path / "current", dbt_options)
        beyond_metadata = modified
        if modified:
            state = json.loads(
                (Path(state_dir) / "manifest.json").read_text(encoding="utf-8")
            )
            current = json.loads(
                (tmp_path / "current" / "manifest.json").read_text(encoding="utf-8")
            )
            mask_state(state, current, categories)
            masked_dir = tmp_path / "masked"
            masked_dir.mkdir()
            (masked_dir / "manifest.json").write_text(
                json.dumps(state), encoding="utf-8"
            )
            beyond_metadata = list_modified(
                masked_dir, tmp_path / "current_again", dbt_options
            )
    return {
        "metadata_only": bool(modified) and not beyond_metadata,
        "categories": categories,
        "modified": modified,
        "modified_beyond_metadata": beyond_metadata,
    }
