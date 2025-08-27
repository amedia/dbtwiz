from datetime import date

from dbtwiz.config.project import project_config
from dbtwiz.dbt.manifest import Manifest
from dbtwiz.dbt.run import invoke
from dbtwiz.integrations.gcp_auth import ensure_auth
from dbtwiz.utils.logger import debug, error, info


def test(
    target: str,
    select: str,
    date: date,
) -> None:
    """Runs tests for models."""
    if target == "dev":
        ensure_auth()

    commands = ["test"]
    args = {
        "target": target,
        "vars": f'{{data_interval_start: "{date}"}}',
    }

    Manifest.models_cached()
    if target == "dev" and not Manifest.can_select_directly(select):
        Manifest().update_models_info()
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
        args["state"] = project_config().docker_image_manifest_path
    else:
        # Running ALL tests means you'd have to build ALL models - not likely
        error("Selector is required with dev target.")
        return

    invoke(commands, **args)
