import subprocess
from pathlib import Path


def models_with_local_changes(models):
    """Return a list of names of models with local changes according to Git"""
    output = subprocess.check_output(["git", "status", "--porcelain"])
    result = list()
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
