import os
from pathlib import Path
from typing import Dict, List, Tuple, Union

from dbtwiz.config.project import project_path

from .model import ModelBasePath


class Group:
    """Project's model groups"""

    YAML_PATH = project_path() / "models" / "model_groups.yml"

    def __init__(self):
        """Initializes the class and reads the model groups"""
        from yaml import safe_load  # Lazy import for improved performance

        with open(self.YAML_PATH, "r") as f:
            data = safe_load(f)
        self.groups = data["groups"]

    def choices(self):
        """Return a dict with group names and descriptions"""
        return dict([(g["name"], g["owner"]["description"]) for g in self.groups])

    def yaml_relative_path(self):
        """Relative path to YAML file for model groups"""
        return self.YAML_PATH.relative_to(project_path())


class Profile:
    """Project's profiles"""
    PROFILES_DIR = (
        Path(os.environ.get("DBT_PROFILES_DIR"))
        if os.environ.get("DBT_PROFILES_DIR")
        else project_path() / ".profiles"
    )
    YAML_PATH = PROFILES_DIR / "profiles.yml"

    def __init__(self):
        """Initializes the class and reads the model groups"""
        from yaml import safe_load  # Lazy import for improved performance

        if not self.YAML_PATH.exists():
            raise FileNotFoundError(
                "Couldn't find profiles.yml. Is the DBT_PROFILES_DIR env var set?"
            )

        with open(self.YAML_PATH, "r", encoding="utf-8") as f:
            self.profiles = safe_load(f)[Project().profile()]["outputs"]

    def _resolve_profile(self, raw_config):
        """Get fully rendered profile values including env_var resolution"""
        from jinja2 import BaseLoader, Environment  # Lazy import for improved performance

        # Create custom renderer
        def render_value(value):
            if not isinstance(value, str):
                return value

            template = Environment(loader=BaseLoader()).from_string(value)
            return template.render(env_var=os.getenv)

        # Return rendered values
        return {k: render_value(v) for k, v in raw_config.items()}

    def profile_config(self, target_name):
        """Return a dict with the profile configuration"""
        return self._resolve_profile(raw_config=self.profiles[target_name])


class Project:
    """Project's variable settings"""

    YAML_PATH = project_path() / "dbt_project.yml"

    def __init__(self):
        from yaml import safe_load  # Lazy import for improved performance

        with open(self.YAML_PATH, "r") as f:
            self.data = safe_load(f)

    def name(self) -> str:
        return self.data.get("name")

    def profile(self) -> str:
        return self.data.get("profile")

    def teams(self) -> List[Dict[str, str]]:
        """List of teams defined in project config"""
        return [
            {"name": key, "description": value.get("description")}
            for key, value in self.data.get("vars", {}).get("teams", {}).items()
        ]

    def service_consumers(self) -> List[Dict[str, str]]:
        """List of service consumers defined in project config"""
        return [
            {"name": key, "description": value.get("description")}
            for key, value in self.data.get("vars", {})
            .get("service-consumers", {})
            .items()
        ]

    def access_policies(self) -> List[Dict[str, str]]:
        """List of access policies defined in project config"""
        return [
            {"name": key, "description": value.get("description")}
            for key, value in self.data.get("vars", {})
            .get("access-policies", {})
            .items()
        ]

    def data_expirations(self) -> List[Dict[str, str]]:
        """List of data expiration policies"""
        return [
            {
                "name": key,
                "description": f"Used for {key.replace('-', ' ').replace(' expiration', '')} ({value} days)",
            }
            for key, value in self.data.get("vars", {}).items()
            if key.endswith("-data-expiration")
        ]


def layer_choices() -> List[Dict[str, str]]:
    """Dict of dbt layers and descriptions"""
    return [
        {
            "name": "staging",
            "description": "Initial building blocks mapping the source data",
        },
        {
            "name": "intermediate",
            "description": "Logic to prepare data to be joined into products at later stages",
        },
        {
            "name": "marts",
            "description": "Data products made available to several consumers",
        },
        {
            "name": "bespoke",
            "description": "Data products tailored to one specific consumer",
        },
    ]


def materialization_choices() -> List[Dict[str, str]]:
    """Dict of dbt materializations and descriptions"""
    return [
        {"name": "view", "description": "Default"},
        {"name": "table", "description": "Typically used for smaller mart models"},
        {"name": "incremental", "description": "Used for large models"},
        {
            "name": "ephemeral",
            "description": "Should very rarely be used, only for logically splitting up the code",
        },
    ]


def access_choices() -> List[Dict[str, str]]:
    """Dict of access levels and descriptions"""
    return [
        {
            "name": "private",
            "description": "Usable only by other models in the same group",
        },
        {"name": "protected", "description": "Usable by models outside the group"},
        {"name": "public", "description": "For marts models"},
    ]


def frequency_choices() -> List[Dict[str, str]]:
    """Dict of frequencies and descriptions"""
    return [
        {"name": "hourly", "description": "Model needs to be updated every hour"},
        {"name": "daily", "description": "Model needs to be updated once a day"},
    ]


def get_source_tables() -> Tuple[
    Dict[str, str], List[Dict[str, Union[str, List[str]]]]
]:
    """Read all existing sources from YAML files in the sources directory.

    Returns: A tuple containing:
            - dict_part (Dict[str, str]): A dictionary of source tables (source_name.table_name: description).
            - list_part (List[Dict[str, Union[str, List[str]]]]): A list of sources (source_name, database, schema, table_names, file).
    """
    from yaml import safe_load  # Lazy import for improved performance

    dbt_source_tables = {}
    dbt_sources = []
    for yml_file in Path("sources").rglob("*"):
        if yml_file.suffix in {".yml", ".yaml"}:
            with open(yml_file, "r") as f:
                content = safe_load(f)
                sources = content.get("sources", [])
                for source in sources:
                    # Add all tables to dict
                    for table in source.get("tables", []):
                        dbt_source_tables[f"{source['name']}.{table['name']}"] = (
                            table.get("description")
                        )
                    # All all sources to list
                    dbt_sources.append(
                        {
                            "name": source["name"],
                            "description": source.get("description"),
                            "project": source["database"],
                            "dataset": source["schema"],
                            "tables": [
                                table["name"] for table in source.get("tables", [])
                            ],
                            "file": yml_file,
                        }
                    )

    dbt_source_tables = dict(sorted(dbt_source_tables.items()))
    dbt_sources = sorted(dbt_sources, key=lambda x: x["name"])

    return dbt_source_tables, dbt_sources


def domains_for_layer(layer: str):
    """List of domains in the given layer for this project"""
    domains = [f.name for f in (project_path() / "models").glob(f"*_{layer}/*")]
    return sorted(domains)


def list_domain_models(base_path: ModelBasePath, domain: str):
    """List of existing models in the given layer/domain for this project"""
    yml_files = []
    for path in base_path.get_domain_path(domain).rglob("*.yml"):
        yml_files.append(str(path.stem))

    return yml_files
