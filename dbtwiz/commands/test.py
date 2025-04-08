from datetime import date

from dbtwiz.dbt.manifest import Manifest
from dbtwiz.dbt.run import invoke
from dbtwiz.gcp.auth import ensure_auth
from dbtwiz.helpers.log_types import debug, error, fatal, info


def test(
    target: str,
    select: str,
    date: date,
) -> None:
    """Runs tests for models."""
    if target != "dev":
        fatal("Test command is only support for use in dev.")

    ensure_auth()

    commands = ["test"]
    args = {
        "target": target,
        "vars": f'{{data_interval_start: "{date}"}}',
    }

    models = Manifest.models_cached()
    if select not in models.keys():
        chosen_models = Manifest.choose_models(select)
        if chosen_models:
            select = ",".join(chosen_models)
            debug(f"Select: '{select}'")
        else:
            select = None

    if select is not None and len(select) > 0:
        info(f"Testing models matching '{select}'.")
        args["select"] = select
    else:
        # Running ALL tests means you'd have to build ALL models - not likely
        error("Selector is required with dev target.")
        return

    invoke(commands, **args)
