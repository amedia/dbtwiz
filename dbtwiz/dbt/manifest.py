import functools
import json
import re
from pathlib import Path
from typing import List

from jinja2 import Template

from dbtwiz.config.project import project_config, project_dbtwiz_path
from dbtwiz.config.user import user_config
from dbtwiz.gcp.auth import ensure_app_default_auth
from dbtwiz.helpers.logger import debug, error, info

from .run import invoke
from .support import models_with_local_changes


class Manifest:
    MANIFEST_PATH = Path(".", "target", "manifest.json")
    PROD_MANIFEST_PATH = project_dbtwiz_path() / "prod-state" / "manifest.json"
    MODELS_CACHE_PATH = project_dbtwiz_path("models-cache.json")
    MODELS_INFO_PATH = project_dbtwiz_path("models")

    def __init__(self, path: Path = MANIFEST_PATH):
        """Initialize the class by loading manifest data from the given path."""
        # TODO: Check that the manifest file exists, and build it if not
        with open(path, "r") as f:
            manifest = json.load(f)
            self.nodes = manifest["nodes"]
            self.sources_nodes = manifest["sources"]
            self.parent_map = manifest["parent_map"]
            self.child_map = manifest["child_map"]

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
        invoke(["parse"], quiet=True)

    @classmethod
    def get_local_manifest_age(cls, manifest_path):
        """Returns the age of the manifest in hours. If it doesn't exist, 999 is returned."""
        manifest_file = Path(manifest_path)
        if manifest_file.is_file():
            from datetime import datetime

            modified_time_float = manifest_file.stat().st_mtime
            modified_time = datetime.fromtimestamp(modified_time_float)
            current_time = datetime.now()
            difference_in_seconds = (current_time - modified_time).total_seconds()

            return int(difference_in_seconds // 3600)
        return 999

    @classmethod
    def download_prod_manifest(cls, force=False):
        """Downloads latest production manifest if force or older than 2 hours."""
        if (
            force
            or cls.get_local_manifest_age(manifest_path=cls.PROD_MANIFEST_PATH) >= 2
        ):
            info("Fetching production manifest")
            from google.cloud import storage  # Only when used

            ensure_app_default_auth()

            gcs = storage.Client(project=project_config().bucket_state_project)
            blob = gcs.bucket(project_config().bucket_state_identifier).blob(
                "manifest.json"
            )
            # Create path if missing
            cls.PROD_MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
            # Download prod manifest to path
            blob.download_to_filename(cls.PROD_MANIFEST_PATH)
            gcs.close()

    @classmethod
    def update_manifests(cls, type, force=False):
        """Rebuild local manifest and download latest production manifest"""
        if type in ("all", "dev"):
            cls.rebuild_manifest()
        if type in ("all", "prod"):
            cls.download_prod_manifest(force=force)

    @classmethod
    def get_manifest(cls, manifest_path):
        """Reads and returns the manifest at the given path."""
        manifest_path = Path(manifest_path)

        if not manifest_path.is_file():
            raise FileNotFoundError(
                f"The file at path '{manifest_path}' does not exist."
            )
        with manifest_path.open("r", encoding="utf-8") as f:
            return json.load(f)

    @classmethod
    def choose_models(
        cls, select: str, multi: bool = True, work: bool = False
    ) -> List[str]:
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
            select in cls.models_cached().keys()
            or
            # select contains special characters
            re.search(r"[:+*, ]", select) is not None
        )

    def update_models_cache(self):
        """Save the current models to the models cache file."""
        Path.mkdir(self.MODELS_CACHE_PATH.parent, exist_ok=True)
        with open(self.MODELS_CACHE_PATH, "w+") as f:
            json.dump(self.models(), f)

    def update_models_info(self):
        """Update model information files based on current models."""
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
        """Generate and return the model information template with optional clearing."""
        with open(Path(__file__).parent / "templates" / "model_info.tpl", "r+") as f:
            template = f.read()
        if clear:
            template = "\033[2J\033[H" + template
        template = template.replace("[b]", "\033[1m")
        template = template.replace("[/]", "\033[0m")
        # template = re.sub(r"\[c(\d+)\]", r"\033[38;5;\1m", template)
        template = re.sub(
            r"\[(\w+)\]", lambda m: f"\033[38;5;{user_config().color(m[1])}m", template
        )
        template_object = Template(template)
        template_object.globals["model_style"] = model_style
        return template_object

    @functools.cache
    def models(self):
        """Retrieve all models with their metadata and relationships."""
        models = dict()
        for key, node in self.nodes.items():
            if node["resource_type"] == "model":
                config = node["config"]
                folder = Path("models", node["path"].replace(node["name"] + ".sql", ""))
                parent_models = self.parent_models(key)
                child_models = self.child_models(key)
                models[node["name"]] = dict(
                    unique_id=key,
                    database=node["database"],
                    schema=node["schema"],
                    name=node["name"],
                    alias=node["alias"],
                    path=node["path"],
                    folder=str(folder),
                    tags=node["tags"],
                    meta=node["meta"],
                    group=node["group"],
                    relation_name=node["relation_name"],
                    description=node["description"],
                    materialized=config["materialized"],
                    parent_models=parent_models,
                    child_models=child_models,
                    deprecated=node["description"].lower().startswith("deprecated"),
                )
        return models

    def model_by_name(self, name):
        """Find and return a model by its name."""
        for model in self.models().values():
            if model["name"] == name:
                return model
        return None

    def parent_models(self, key):
        """Get and return the sorted list of parent models for the given key."""
        parents = [
            self.nodes[nk]["name"]
            for nk in self.parent_map[key]
            if nk in self.nodes and self.nodes[nk]["resource_type"] == "model"
        ]
        return sorted(parents, key=self.model_ordering)

    def child_models(self, key):
        """Get and return the sorted list of child models for the given key."""
        children = [
            self.nodes[nk]["name"]
            for nk in self.child_map[key]
            if nk in self.nodes and self.nodes[nk]["resource_type"] == "model"
        ]
        return sorted(children, key=self.model_ordering)

    def model_ordering(self, name):
        """Determine and return the ordering key for the given model name."""
        if name.startswith("stg_"):
            return f"0_{name}"
        elif name.startswith("int_"):
            return f"1_{name}"
        else:
            return f"2_{name}"

    @functools.cache
    def model_dependencies_upstream(self, model_name):
        """Retrieve the upstream dependencies for the given model name."""
        parent_models = list(
            filter(lambda node: node.startswith("model."), self.parent_map[model_name])
        )
        dependencies = set()
        for parent in parent_models:
            if parent not in dependencies:
                node_config = self.nodes[parent]["config"]
                materialized = node_config.get("materialized", None)
                # if materialized in (["table", "incremental"]):
                dependencies.add((parent, materialized))
                dependencies.update(self.model_dependencies_upstream(parent))
        return dependencies

    @functools.cache
    def model_dependencies_downstream(self, model_name):
        """Retrieve the downstream dependencies for the given model name."""
        children = list(
            filter(lambda node: node.startswith("model."), self.child_map[model_name])
        )
        dependencies = set()
        for child in children:
            if child not in dependencies:
                node_config = self.nodes[child]["config"]
                materialized = node_config.get("materialized", None)
                # if materialized in (["table", "incremental"]):
                dependencies.add((child, materialized))
                dependencies.update(self.model_dependencies_downstream(child))
        return dependencies

    @functools.cache
    def sources(self):
        """Retrieve all models with their metadata and relationships."""
        sources = dict()
        for key, node in self.sources_nodes.items():
            if node["resource_type"] == "source":
                sources[node["name"]] = dict(
                    unique_id=key,
                    database=node["database"],
                    schema=node["schema"],
                    name=node["name"],
                    source_name=node["source_name"],
                    source_description=node["source_description"],
                    identifier=node["identifier"],
                    path=node["path"],
                    folder="sources",
                    description=node["description"],
                    tags=node["tags"],
                    meta=node["meta"],
                    config=node["config"],
                    source_meta=node["source_meta"],
                )
        return sources


def model_style(name: str):
    """Determine and return the style color for the given model name."""
    if name.startswith("stg_"):
        key = "dep_stg"
    if name.startswith("int_"):
        key = "dep_int"
    else:
        key = "dep_mart"
    cval = user_config().color(key)
    return f"\033[38;5;{cval}m"
