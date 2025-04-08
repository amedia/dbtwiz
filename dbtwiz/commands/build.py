import json
import os
from datetime import date, timedelta

from dbtwiz.config.project import project_dbtwiz_path
from dbtwiz.dbt.manifest import Manifest
from dbtwiz.dbt.run import invoke
from dbtwiz.gcp.auth import ensure_auth
from dbtwiz.helpers.log_types import debug, error, fatal, info

LAST_SELECT_FILE = project_dbtwiz_path("last_select.json")


def choose_models(select, repeat_last: bool, work=None):
    """
    Determine the chosen models based on the target, selection, and repeat_last flag.
    """
    if Manifest.can_select_directly(select):
        chosen_models = [select]
    elif repeat_last:
        chosen_models = load_selected_models()
    else:
        Manifest().update_models_info()
        chosen_models = Manifest.choose_models(select, work=work)

    return chosen_models


def build(
    target: str,
    select: str,
    date: date,
    use_task_index: bool,
    full_refresh: bool,
    upstream: bool,
    downstream: bool,
    work: bool,
    repeat_last: bool,
) -> None:
    """Builds the given models."""
    if target != "dev":
        fatal("Build command is only support for use in dev.")

    ensure_auth()

    chosen_models = choose_models(target, select, repeat_last, work)
    if chosen_models is None:
        error("No models selected.")
        return

    save_selected_models(chosen_models)

    select = ""
    chosen_models_with_deps = [
        "".join(["+" if upstream else "", model, "+" if downstream else ""])
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
        "vars": f'{{data_interval_start: "{date}"}}',
    }

    if len(select) > 0:
        info(f"Building models matching '{select}'.")
        args["select"] = select
    else:
        error("Selector is required with dev target.")
        return

    if full_refresh:
        info("Full refresh requested.")
        args["full-refresh"] = True

    if use_task_index:
        # No need for artifacts when backfilling
        args["write-json"] = False

    invoke(commands, **args)


def save_selected_models(models):
    """Saves the selected models."""
    with open(LAST_SELECT_FILE, "w+") as f:
        f.write(json.dumps(models))


def load_selected_models():
    """Loads the selected models."""
    if not LAST_SELECT_FILE.exists():
        error("No previously selected models found.")
        return None
    with open(LAST_SELECT_FILE, "r") as f:
        models = json.loads(f.read())
    return models
