import json
import os
import shutil
import subprocess
from typing import Any, List

from ..config.project import project_config
from ..utils.logger import debug, fatal


def dbt_executable() -> str:
    """The dbt CLI to invoke.

    dbt v2 is a standalone binary and exposes no Python API, so dbt is always
    driven as a subprocess rather than through dbt-core's `dbtRunner`. The same
    call works for dbt-core, whose CLI accepts every flag used here, which lets a
    project migrate to v2 by putting the v2 binary first on PATH and changing
    nothing in dbtwiz. Set DBT_EXECUTABLE to pick a specific one when both are
    installed.
    """
    executable = os.environ.get("DBT_EXECUTABLE") or "dbt"
    return shutil.which(executable) or executable


def dbt_args(commands: List[str], **args: Any) -> List[str]:
    """Render a dbt command and its keyword arguments as a CLI argument list.

    Underscores in keyword names become hyphens, and a boolean renders as the
    `--flag` / `--no-flag` pair dbt uses for toggles.
    """
    rendered = list(commands)
    for key, value in args.items():
        key = key.replace("_", "-")
        if isinstance(value, bool):
            rendered.append(f"--{key}" if value else f"--no-{key}")
        else:
            rendered.extend([f"--{key}", value])

    # Add project directory to dbt args
    rendered.extend(["--project-dir", str(project_config().root_path())])
    return rendered


def invoke(commands: List[str], **args: Any) -> None:
    """Invoke a dbt run.

    Args:
        commands: List of dbt commands to execute
        **args: Additional arguments to pass to dbt

    Raises:
        SystemExit: If dbt invocation fails (via fatal function)
    """
    if args.get("target", "dev") != "dev":
        args["use-colors"] = False
        args["profiles-dir"] = project_config().docker_image_profiles_path

    command = [dbt_executable()] + dbt_args(commands, **args)
    debug(f"Invoking dbt with args: {command[1:]}")

    # Output is inherited rather than captured: dbt's progress is the point of a
    # build, and a long run must not sit silent until it finishes.
    result = subprocess.run(command)

    if result.returncode != 0:
        fatal(f"dbt invocation failed with exit code {result.returncode}.", exit_code=1)


def get_selected_models(select: str) -> List[dict]:
    """Returns the models identified by the given dbt select statement."""
    command = [dbt_executable()] + dbt_args(
        ["ls"],
        # --quiet keeps the banner, warnings and summary off stdout, so what is
        # captured below is nothing but the JSON lines.
        quiet=True,
        resource_type="model",
        select=select,
        output="json",
    )
    debug(f"Invoking dbt with args: {command[1:]}")

    result = subprocess.run(command, capture_output=True, text=True)

    if result.returncode != 0:
        fatal(
            f"dbt invocation failed with exit code {result.returncode}:\n"
            f"{result.stderr or result.stdout}",
            exit_code=2,
        )

    models = []
    for line in result.stdout.splitlines():
        line = line.strip()
        if line.startswith("{"):
            models.append(json.loads(line))
    return models
