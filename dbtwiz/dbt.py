from typing import List

from .logging import debug, fatal


def dbt_invoke(commands: List[str], **args: dict):

    if args.get("target", "dev") != "dev":
        args["use-colors"] = False
        args["profiles-dir"] = "/app/.profiles"

    dbt_args = [c for c in commands]
    for key, value in args.items():
        key = key.replace("_", "-")
        if isinstance(value, bool):
            dbt_args.append(f"--{key}" if value else f"--no-{key}")
        else:
            dbt_args.extend([f"--{key}", value])

    # this import takes almost 2s, so wait until we actually use it
    from dbt.cli.main import dbtRunner, dbtRunnerResult
    debug(f"Invoking dbt with args: {dbt_args}")
    result = dbtRunner().invoke(dbt_args)

    if not result.success:
        if result.exception:
            fatal(result.exception, exit_code=2)
        else:
            fatal("dbt invocation failed.", exit_code=1)
