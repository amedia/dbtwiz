from datetime import date

from dbtwiz.auth import ensure_auth
from dbtwiz.dbt import dbt_invoke
from dbtwiz.logging import debug, error, fatal, info
from dbtwiz.manifest import Manifest


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

    dbt_invoke(commands, **args)
