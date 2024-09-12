from datetime import date, timedelta
import json
import os
from pathlib import Path

from google.cloud import storage

from .auth import ensure_auth
from .config import project_config, project_dbtwiz_path
from .dbt import dbt_invoke
from .logging import info, debug, error, fatal
from .manifest import Manifest


class Build():

    VALID_TARGETS = ["dev", "build", "prod-ci", "prod"]

    LAST_SELECT_FILE = project_dbtwiz_path("last_select.json")


    @classmethod
    def run(cls,
            target: str,
            select: str,
            date: date,
            use_task_index: bool,
            save_state: bool,
            full_refresh: bool,
            upstream: bool,
            downstream: bool,
            work: bool,
            repeat_last: bool,
    ) -> None:

        if target == "dev":
            ensure_auth()

        if target != "dev" or Manifest.can_select_directly(select):
            chosen_models = [select]
        elif repeat_last:
            chosen_models = cls.load_selected_models()
        else:
            Manifest().update_models_info()
            chosen_models = Manifest.choose_models(select, work=work)

        if chosen_models is None:
            error("No models chosen.")
            return

        cls.save_selected_models(chosen_models)

        select = ""
        chosen_models_with_deps = [
            "".join([
                "+" if upstream else "",
                model,
                "+" if downstream else ""
            ])
            for model in chosen_models
        ]

        select = " ".join(chosen_models_with_deps)
        debug(f"Select: '{select}'")

        if use_task_index:
            date_offset = int(os.environ.get("CLOUD_RUN_TASK_INDEX", 0))
            info(f"Using CLOUD_RUN_TASK_INDEX={date_offset}")
            info(f"Base date: {date}")
            date += timedelta(days=date_offset)
            info(f"Backfill date: {date}")

        commands = ["build"]
        args = {
            "target": target,
            "vars": f"{{data_interval_start: \"{date}\"}}",
        }

        if len(select) > 0:
            info(f"Building models matching '{select}'.")
            args["select"] = select
        elif target != "dev":
            info("Builing modified models and their downstream dependencies.")
            args["select"] = "state:modified+"
            args["defer"] = True
            args["state"] = project_config().pod_manifest_path
        else:
            error("Selector is required with dev target.")
            return

        if full_refresh:
            info("Full refresh requested.")
            args["full-refresh"] = True

        if use_task_index:
            # No need for artifacts when backfilling
            args["write-json"] = False

        dbt_invoke(commands, **args)

        if save_state and target != "dev":
            info("Saving state, uploading manifest to bucket.")
            gcs = storage.Client(project=project_config().gcp_project)
            blob = gcs.bucket(project_config().dbt_state_bucket).blob("manifest.json")
            blob.upload_from_filename("./target/manifest.json")


    @classmethod
    def save_selected_models(cls, models):
        Path.mkdir(project_dbtwiz_path(), exist_ok=True)
        with open(cls.LAST_SELECT_FILE, "w+") as f:
            f.write(json.dumps(models))


    @classmethod
    def load_selected_models(cls):
        if not cls.LAST_SELECT_FILE.exists():
            error("No previously selected models found.")
            return None
        with open(cls.LAST_SELECT_FILE, "r") as f:
            models = json.loads(f.read())
        return models
