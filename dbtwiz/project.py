from pathlib import Path
from typing import Dict, List, Tuple, Union

from dbtwiz.logging import fatal

from .config import project_path


class Group:
    """Project's model groups"""

    YAML_PATH = project_path() / "models" / "model_groups.yml"

    def __init__(self):
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


class Project:
    """Project's variable settings"""

    YAML_PATH = project_path() / "dbt_project.yml"

    def __init__(self):
        from yaml import safe_load  # Lazy import for improved performance

        with open(self.YAML_PATH, "r") as f:
            data = safe_load(f)
            self.data = data.get("vars", {})

    def teams(self) -> List[Dict[str, str]]:
        """List of teams defined in project config"""
        return [
            {"name": key, "description": value.get("description")}
            for key, value in self.data.get("teams", {}).items()
        ]

    def service_consumers(self) -> List[Dict[str, str]]:
        """List of service consumers defined in project config"""
        return [
            {"name": key, "description": value.get("description")}
            for key, value in self.data.get("service-consumers", {}).items()
        ]

    def access_policies(self) -> List[Dict[str, str]]:
        """List of access policies defined in project config"""
        return [
            {"name": key, "description": value.get("description")}
            for key, value in self.data.get("access-policies", {}).items()
        ]

    def data_expirations(self) -> List[Dict[str, str]]:
        """List of data expiration policies"""
        return [
            {
                "name": key,
                "description": f"Used for {key.replace('-', ' ').replace(' expiration', '')} ({value} days)",
            }
            for key, value in self.data.items()
            if key.endswith("-data-expiration")
        ]


class ModelBasePath:
    """Path to model files (without suffix)"""

    def __init__(self, layer: str):
        self.layer = layer
        self.layer_folder, self.layer_abbreviation = self.get_layer_details(layer)

    def get_layer_details(self, layer: str):
        layer_details = {
            "staging": ("1_staging", "stg"),
            "intermediate": ("2_intermediate", "int"),
            "marts": ("3_marts", "mrt"),
            "bespoke": ("4_bespoke", "bsp")
        }
        if layer not in layer_details:
            raise ValueError(f"Invalid layer: {layer}")
        return layer_details[layer]

    def get_path(self, domain: str, name: str) -> Path:
        path = project_path() / "models" / self.layer_folder / domain / f"{self.layer_abbreviation}_{domain}__{name}"
        return path

    def get_prefix(self, domain: str) -> str:
        prefix = f"{self.layer_abbreviation}_{domain}__"
        return prefix


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
    for yml_file in Path("sources").iterdir():
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
