from datetime import date

from dbtwiz.auth import ensure_auth
from dbtwiz.config import project_config
from dbtwiz.dbt import dbt_invoke
from dbtwiz.logging import debug, error, info
from dbtwiz.manifest import Manifest

VALID_TARGETS = ["dev", "build", "prod-ci", "prod"]


def test(
    target: str,
    select: str,
    date: date,
) -> None:
    if target == "dev":
        ensure_auth()

    commands = ["test"]
    args = {
        "target": target,
        "vars": f'{{data_interval_start: "{date}"}}',
    }

    models = Manifest.models_cached()
    if target == "dev" and select not in models.keys():
        chosen_models = Manifest.choose_models(select)
        if chosen_models:
            select = ",".join(chosen_models)
            debug(f"Select: '{select}'")
        else:
            select = None

    if select is not None and len(select) > 0:
        info(f"Testing models matching '{select}'.")
        args["select"] = select
    elif target != "dev":
        info("Testing modified models and their downstream dependencies.")
        args["select"] = "state:modified+"
        args["defer"] = True
        args["state"] = project_config().pod_manifest_path
    else:
        # Running ALL tests means you'd have to build ALL models - not likely
        error("Selector is required with dev target.")
        return

    dbt_invoke(commands, **args)
