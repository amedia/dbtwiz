from enum import Enum
from pathlib import Path
from typing import Dict, List
import os
import yaml

from dbtwiz.logging import fatal

from .config import project_path


def layer_choices() -> List[Dict[str, str]]:
    """Dict of dbt layers and descriptions"""
    return [
        {"name": "staging", "description": "Initial building blocks mapping the source data"},
        {"name": "intermediate", "description": "Logic to prepare data to be joined into products at later stages"},
        {"name": "marts", "description": "Data products made available to several consumers"},
        {"name": "bespoke", "description": "Data products tailored to one specific consumer"},
    ]


def materialization_choices() -> List[Dict[str, str]]:
    """Dict of dbt materializations and descriptions"""
    return [
        {"name": "view", "description": "Default"},
        {"name": "table", "description": "Typically used for smaller mart models"},
        {"name": "incremental", "description": "Used for large models"},
        {"name": "ephemeral", "description": "Should very rarely be used, only for logically splitting up the code"},
    ]


def access_choices() -> List[Dict[str, str]]:
    """Dict of access levels and descriptions"""
    return [
        {"name": "private", "description": "Usable only by other models in the same group"},
        {"name": "protected", "description": "Usable by models outside the group"},
        {"name": "public", "description": "For marts models"},
    ]


def frequency_choices() -> List[Dict[str, str]]:
    """Dict of frequencies and descriptions"""
    return [
        {"name": "hourly", "description": "Model needs to be updated every hour"},
        {"name": "daily", "description": "Model needs to be updated once a day"},
    ]


class Group:
    """Project's model groups"""

    YAML_PATH = project_path() / "models" / "model_groups.yml"

    def __init__(self):
        with open(self.YAML_PATH, "r") as f:
            data = yaml.safe_load(f)
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
        with open(self.YAML_PATH, "r") as f:
            data = yaml.safe_load(f)
            self.data = data.get("vars", {})

    def teams(self) -> List[str]:
        """List of teams defined in project config"""
        return [
            {"name": key, "description": value.get("description")}
            for key, value in self.data.get("teams", {}).items()
        ]

    def service_consumers(self) -> List[str]:
        """List of service consumers defined in project config"""
        return [
            {"name": key, "description": value.get("description")}
            for key, value in self.data.get("service-consumers", {}).items()
        ]

    def access_policies(self) -> List[str]:
        """List of access policies defined in project config"""
        return [
            {"name": key, "description": value.get("description")}
            for key, value in self.data.get("access-policies", {}).items()
        ]

    def data_expirations(self) -> List[str]:
        """List of data expiration policies"""
        return [
            {"name": item, "description": f"Used for {item.replace('-', ' ').replace(' expiration', '')}"}
            for item in self.data.keys() if item.endswith("-data-expiration")
        ]

def get_source_tables():
    """List of sources in this project"""
    source_tables = {}
    for root, _, files in os.walk(project_path() / "sources"):
        for file in files:
            if file.endswith('.yml') or file.endswith('.yaml'):
                file_path = os.path.join(root, file)
                with open(file_path, 'r') as f:
                    content = yaml.safe_load(f)
                    sources = content.get('sources', [])
                    for source in sources:
                        for table in source.get('tables', []):
                            source_tables[f"{source['name']}.{table['name']}"] = table.get("description")
    return source_tables


def domains_for_layer(layer: str):
    """List of domains in the given layer for this project"""
    domains = [f.name for f in (project_path() / "models").glob(f"*_{layer}/*")]
    return sorted(domains)

      
def model_base_path(layer: str, domain: str, name :str) -> Path:
    """Path to model files (without suffix)"""
    path = project_path() / "models"
    if layer == "staging":
        path = path / "1_staging" / domain / f"stg_{domain}__{name}"
    elif layer == "intermediate":
        path = path / "2_intermediate" / domain / f"int_{domain}__{name}"
    elif layer == "marts":
        path = path / "3_marts" / domain / f"mrt_{domain}__{name}"
    elif layer == "bespoke":
        path = path / "4_bespoke" / domain / f"bsp_{domain}__{name}"
    else:
        fatal(f"Invalid layer: [red]{layer}[/]")
    return path
