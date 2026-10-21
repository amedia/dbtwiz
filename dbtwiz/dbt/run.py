import json
import os
import shutil
import subprocess
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
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


def dbt_core_installed() -> bool:
    """Whether dbt-core 1.x is installed, so sqlfluff's dbt templater can run.

    Without it the project is on the dbt v2 engine only, and linting goes through
    `dbt lint`, which reads the same .sqlfluff config. dbt-core 2.x is the wheel
    distribution of the v2 engine, so it does not count.
    """
    try:
        return int(version("dbt-core").split(".")[0]) < 2
    except PackageNotFoundError:
        return False


def disable_sqlfluff_dbt_version_check() -> None:
    """Turn off dbt's package version check when sqlfluff's dbt templater compiles.

    The templater builds dbt's arguments itself, with no setting for the version
    check, and dbt-core reads it as `getattr(args, "VERSION_CHECK", True)`, so it is
    always on there, whatever DBT_VERSION_CHECK says. A package that requires another
    dbt version (e.g. one holding only dbt v2 checks) then stops every lint. Linting
    only compiles the SQL, and dbt's own commands still check versions.
    """
    try:
        from sqlfluff_templater_dbt.templater import DbtConfigArgs
    except ImportError:
        return
    DbtConfigArgs.VERSION_CHECK = False  # type: ignore[attr-defined]


def dbt_lint_command(
    files: List[str], fix: bool = False, json_output: bool = False
) -> List[str]:
    """The `dbt lint` command for the given files.

    `--format json` silently disables `--fix`, so fixing and reading back the
    violations that remain have to be two separate invocations.
    """
    if fix and json_output:
        raise ValueError("dbt lint does not fix with --format json")
    command = [dbt_executable(), "lint", "--quiet"]
    if fix:
        command.append("--fix")
    if json_output:
        command.extend(["--format", "json"])
    command.extend(["--project-dir", str(project_config().root_path())])
    return command + [str(f) for f in files]


def dbt_check_command(model_name: str, target_path: Path) -> List[str]:
    """The `dbt check` command for one model (dbt v2 only).

    The results are read back from run_results.json in `target_path`, which keeps
    them apart from the project's own target folder and whatever ran there last.
    """
    return [
        dbt_executable(),
        "check",
        "--quiet",
        "--select",
        model_name,
        "--project-dir",
        str(project_config().root_path()),
        "--target-path",
        str(target_path),
    ]


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
    env = dict(os.environ)
    if args.get("target", "dev") != "dev":
        # Colour off through the environment rather than `--no-use-colors`: dbt v2
        # warns that the flag is no longer supported (dbt1700) and points at
        # NO_COLOR instead. Non-dev targets are CI/prod logs, where colour codes
        # are noise.
        env["NO_COLOR"] = "1"
        args["profiles-dir"] = project_config().docker_image_profiles_path

    command = [dbt_executable()] + dbt_args(commands, **args)
    debug(f"Invoking dbt with args: {command[1:]}")

    # Output is inherited rather than captured: dbt's progress is the point of a
    # build, and a long run must not sit silent until it finishes.
    result = subprocess.run(command, env=env)

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
