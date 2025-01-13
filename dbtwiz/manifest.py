import functools
import json
import os
import re
from jinja2 import Template
from pathlib import Path
from typing import List

from google.cloud import storage

from .config import project_config, user_config, project_dbtwiz_path
from .logging import info, debug, error
from .dbt import dbt_invoke
from .support import models_with_local_changes


class Manifest:

    MANIFEST_PATH = Path(".", "target", "manifest.json")
    PROD_MANIFEST_PATH = project_dbtwiz_path("prod-manifest.json")
    MODELS_CACHE_PATH = project_dbtwiz_path("models-cache.json")
    MODELS_INFO_PATH = project_dbtwiz_path("models")


    @classmethod
    def models_cached(cls):
        """Get dictionary of models in local manifest, with JSON file for caching"""
        if not cls.MODELS_CACHE_PATH.exists() or (
                cls.MODELS_CACHE_PATH.stat().st_mtime < cls.MANIFEST_PATH.stat().st_mtime
        ):
            debug("Updating models cache")
            Manifest().update_models_cache()
        with open(cls.MODELS_CACHE_PATH, "r") as f:
            return json.load(f)


    @classmethod
    def rebuild_manifest(cls):
        """Rebuild local manifest"""
        info("Parsing development manifest")
        dbt_invoke(["parse"], quiet=True)


    @classmethod
    def get_prod_manifest(cls):
        """Download latest production manifest"""
        info("Fetching production manifest")
        gcs = storage.Client(project=project_config().gcp_project)
        blob = gcs.bucket(project_config().dbt_state_bucket).blob("manifest.json")
        # Create path if missing
        Path(cls.PROD_MANIFEST_PATH).mkdir(parents=True, exist_ok=True)
        # Download prod manifest to path
        blob.download_to_filename(cls.PROD_MANIFEST_PATH)
        gcs.close()


    @classmethod
    def update_manifests(cls, type):
        """Rebuild local manifest and download latest production manifest"""
        if type in ('all', 'local'):
            cls.rebuild_manifest()
        if type in ('all', 'prod'):
            cls.get_prod_manifest()


    @classmethod
    def choose_models(cls, select: str, multi: bool = True, work: bool = False) -> List[str]:
        """Let user interactively select one or more models using fuzzy search"""
        import iterfzf  # Only when used
        model_info_path = cls.MODELS_INFO_PATH / "{}.txt"
        models = cls.models_cached()
        if work:
            model_names = models_with_local_changes(models)
            if len(model_names) == 0:
                error("No new or modified models found.")
                return None
        else:
            model_names = models.keys()
        formatter = user_config().get("model_info", "formatter")
        chosen_models = iterfzf.iterfzf(
            model_names,
            query=select,
            prompt="Select models > ",
            multi=multi,
            sort=True,
            ansi=True,
            preview=f"{formatter} '{model_info_path}'",
            __extra__=["--preview-window=right,wrap"],
        )
        return chosen_models


    @classmethod
    def can_select_directly(cls, select: str) -> bool:
        """The given select string should be passed on to dbt without interaction"""
        return (
            # select matches name of an existing model exactly
            select in cls.models_cached().keys() or
            # select contains special characters
            re.search(r"[:+*, ]", select) is not None
        )


    def __init__(self, path: Path = MANIFEST_PATH):
        # TODO: Check that the manifest file exists, and build it if not
        with open(path, "r") as f:
            manifest = json.load(f)
            self.nodes = manifest["nodes"]
            self.parent_map = manifest["parent_map"]
            self.child_map = manifest["child_map"]


    def update_models_cache(self):
        Path.mkdir(self.MODELS_CACHE_PATH.parent, exist_ok=True)
        with open(self.MODELS_CACHE_PATH, "w+") as f:
            json.dump(self.models(), f)


    def update_models_info(self):
        Path.mkdir(self.MODELS_INFO_PATH, exist_ok=True)
        for model in self.models().values():
            model_name = model["name"]
            info_file = self.MODELS_INFO_PATH / f"{model_name}.txt"
            if self.model_info_up_to_date(model, info_file):
                continue
            debug(f"Rendering model info to {info_file}")
            model_info = self.model_info_template(clear=True).render(model=model)
            with open(info_file, "w+") as f:
                # combine multiple blank lines into one to avoid
                # painful handling of it in template
                f.write(re.sub(r"\n\n+", "\n\n", model_info))


    def model_info_up_to_date(self, model, info_file) -> bool:
        """Is rendered model info up to date?"""
        if not info_file.exists():
            return False
        info_mtime = info_file.stat().st_mtime
        for extension in [".sql", ".yml"]:
            ext_file = Path(model["folder"]) / (model["name"] + extension)
            if ext_file.exists() and ext_file.stat().st_mtime > info_mtime:
                return False
        return True


    @functools.cache
    def model_info_template(self, clear=False) -> Template:
        with open(Path(__file__).parent / "templates" / "model_info.tpl", "r+") as f:
            template = f.read()
        if clear:
            template = "\033[2J\033[H" + template
        template = template.replace("[b]", "\033[1m")
        template = template.replace("[/]", "\033[0m")
        # template = re.sub(r"\[c(\d+)\]", r"\033[38;5;\1m", template)
        template = re.sub(r"\[(\w+)\]",
                          lambda m: f"\033[38;5;{user_config().color(m[1])}m",
                          template)
        template_object = Template(template)
        template_object.globals["model_style"] = model_style
        return template_object


    @functools.cache
    def models(self):
        models = dict()
        for key, node in self.nodes.items():
            if node["resource_type"] == "model":
                config = node["config"]
                folder = Path("models", node["path"].replace(
                    node["name"] + ".sql", ""))
                parent_models = self.parent_models(key)
                child_models = self.child_models(key)
                models[node["name"]] = dict(
                    database=node["database"],
                    schema=node["schema"],
                    name=node["name"],
                    path=node["path"],
                    folder=str(folder),
                    tags=node["tags"],
                    meta=node["meta"],
                    group=node["group"],
                    description=node["description"],
                    materialized=config["materialized"],
                    parent_models=parent_models,
                    child_models=child_models,
                    deprecated=node["description"].lower().startswith("deprecated"),
                )
        return models


    def model_by_name(self, name):
        for model in self.models().values():
            if model["name"] == name:
                return model
        return None


    def parent_models(self, key):
        parents = [
            self.nodes[nk]["name"]
            for nk in self.parent_map[key]
            if nk in self.nodes and self.nodes[nk]["resource_type"] == "model"
        ]
        return sorted(parents, key=self.model_ordering)


    def child_models(self, key):
        children = [
            self.nodes[nk]["name"]
            for nk in self.child_map[key]
            if nk in self.nodes and self.nodes[nk]["resource_type"] == "model"
        ]
        return sorted(children, key=self.model_ordering)


    def model_ordering(self, name):
        if name.startswith("stg_"):
            return f"0_{name}"
        elif name.startswith("int_"):
            return f"1_{name}"
        else:
            return f"2_{name}"


    @functools.cache
    def model_dependencies_upstream(self, model_name):
        parent_models = list(filter(
            lambda node: node.startswith("model."),
            self.parent_map[model_name]))
        dependencies = set()
        for parent in parent_models:
            if parent not in dependencies:
                node_config = self.nodes[parent]["config"]
                materialized = node_config.get("materialized", None)
                #if materialized in (["table", "incremental"]):
                dependencies.add((parent, materialized))
                dependencies.update(self.model_dependencies_upstream(parent))
        return dependencies


    @functools.cache
    def model_dependencies_downstream(self, model_name):
        children = list(filter(
            lambda node: node.startswith("model."),
            self.child_map[model_name]))
        dependencies = set()
        for child in children:
            if child not in dependencies:
                node_config = self.nodes[child]["config"]
                materialized = node_config.get("materialized", None)
                #if materialized in (["table", "incremental"]):
                dependencies.add((child, materialized))
                dependencies.update(self.model_dependencies_downstream(child))
        return dependencies


def model_style(name: str):
    if name.startswith("stg_"):
        key = "dep_stg"
    if name.startswith("int_"):
        key = "dep_int"
    else:
        key = "dep_mart"
    cval = user_config().color(key)
    return f"\033[38;5;{cval}m"
