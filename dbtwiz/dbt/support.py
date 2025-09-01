import subprocess
from pathlib import Path
from typing import Any, Dict, List


def models_with_local_changes(models: Dict[str, Dict[str, Any]]) -> List[str]:
    """Return a list of names of models with local changes according to Git.

    Args:
        models: Dictionary of models with their metadata

    Returns:
        List of model names that have local changes
    """
    output = subprocess.check_output(["git", "status", "--porcelain"])
    result: List[str] = []
    model_name_by_path = dict(
        [[str(Path("models", m["path"])), m["name"]] for m in models.values()]
    )
    for line in output.decode("utf-8").splitlines():
        stage, *_, path = line.split(" ")
        if stage in "AM" and path.startswith("models") and path.endswith(".sql"):
            name = model_name_by_path.get(path, None)
            if name:
                result.append(name)
    return result
