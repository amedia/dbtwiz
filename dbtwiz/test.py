from datetime import date
import os

from google.cloud import storage

from .auth import ensure_auth
from .dbt import dbt_invoke
from .manifest import Manifest
from .logging import info, debug, error


class Test():

    VALID_TARGETS = ["dev", "build", "prod-ci", "prod"]


    @classmethod
    def run(cls,
            target: str,
            select: str,
            date: date,
    ) -> None:

        if target == "dev":
            ensure_auth()

        commands = ["test"]
        args = {
            "target": target,
            "vars": f"{{data_interval_start: \"{date}\"}}",
        }

        models = Manifest.models_cached()
        if target == "dev" and select not in models.keys():
            chosen_models = Manifest.choose_models(select)
            select = ",".join(chosen_models)
            debug(f"Select: '{select}'")

        if select is not None and len(select) > 0:
            info(f"Testing models matching '{select}'.")
            args["select"] = select
        elif target != "dev":
            info("Testing modified models and their downstream dependencies.")
            args["select"] = "state:modified+"
            args["defer"] = True
            args["state"] = "/app/.manifest"
        else:
            error("Selector is required with dev target.")
            return

        dbt_invoke(commands, **args)
