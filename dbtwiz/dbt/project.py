from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

from dbtwiz.config.project import project_path


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


class Project:
    """Project's variable settings"""

    YAML_PATH = project_path() / "dbt_project.yml"

    def __init__(self):
        from yaml import safe_load  # Lazy import for improved performance

        with open(self.YAML_PATH, "r") as f:
            self.data = safe_load(f)

    def name(self) -> str:
        return self.data.get("name")

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


class ModelBasePath:
    """Path to model files with flexible initialization (layer or path)"""

    def __init__(
        self, layer: Optional[str] = None, path: Optional[Union[str, Path]] = None
    ):
        if path is not None:
            self._init_with_path(path)
        elif layer is not None:
            self._init_with_layer(layer)
        else:
            raise ValueError("Must provide either layer or path")

    def _init_with_layer(self, layer: str):
        """Initialize with just layer name"""
        if layer not in self.layer_details:
            raise ValueError(f"Invalid layer: {layer}")
        self._layer = layer

    def _init_with_path(self, path: Union[str, Path]):
        """Initialize with model path and extract all metadata"""
        path = Path(path)
        self._original_path = path
        self.path = path.parent / path.stem  # Store without extension

        parts = self.path.parts
        try:
            models_pos = parts.index("models")
            if len(parts) > models_pos + 2:
                # Extract folder structure
                layer_folder = parts[models_pos + 1]
                self._domain = parts[models_pos + 2]
                self._model_name = "_".join(parts[models_pos + 3 :])

                # Set layer info
                self._layer, self._layer_abbreviation = next(
                    (
                        (layer, abbr)
                        for layer, (folder, abbr) in self.layer_details.items()
                        if folder == layer_folder
                    ),
                    (None, None),
                )

                # Set prefix and identifier
                expected_prefix = f"{self._layer_abbreviation}_{self._domain}__"
                if self._model_name.startswith(expected_prefix):
                    self._prefix = expected_prefix
                    self._identifier = self._model_name[len(expected_prefix) :]
                else:
                    self._prefix = ""
                    self._identifier = self._model_name

                # Set paths
                self._domain_path = (
                    project_path() / "models" / layer_folder / self._domain
                )
                self._full_path = self._domain_path / self._model_name
        except (ValueError, IndexError) as e:
            raise ValueError(f"Invalid model path structure: {path}") from e

    @property
    def layer_details(self):
        return {
            "staging": ("1_staging", "stg"),
            "intermediate": ("2_intermediate", "int"),
            "marts": ("3_marts", "mrt"),
            "bespoke": ("4_bespoke", "bsp"),
        }

    @property
    def layer(self) -> str:
        if not hasattr(self, "_layer"):
            raise AttributeError("Layer not available")
        return self._layer

    @property
    def layer_folder(self) -> str:
        return self.layer_details[self.layer][0]

    @property
    def layer_abbreviation(self) -> str:
        return self.layer_details[self.layer][1]

    @property
    def domain(self) -> str:
        if not hasattr(self, "_domain"):
            raise AttributeError("Domain not available - initialize with path")
        return self._domain

    @property
    def identifier(self) -> str:
        """The base model name without prefix (e.g. 'customers')"""
        if not hasattr(self, "_identifier"):
            raise AttributeError("Identifier not available - initialize with path")
        return self._identifier

    @property
    def model_name(self) -> str:
        """The full model name with prefix (e.g. 'stg_marketing__customers')"""
        if not hasattr(self, "_model_name"):
            raise AttributeError("Model name not available - initialize with path")
        return self._model_name

    @property
    def prefix(self) -> str:
        if not hasattr(self, "_prefix"):
            raise AttributeError("Prefix not available - initialize with path")
        return self._prefix

    def get_prefix(self, domain: Optional[str] = None) -> str:
        if domain:
            return f"{self.layer_abbreviation}_{domain}__"
        if hasattr(self, "_prefix"):
            return self._prefix
        raise AttributeError("Must provide domain when initialized with layer")

    def get_domain_path(self, domain: Optional[str] = None) -> Path:
        domain = domain or (self._domain if hasattr(self, "_domain") else None)
        if not domain:
            raise AttributeError("Must provide domain when initialized with layer")
        if domain == getattr(self, "_domain", None):
            return self._domain_path
        return project_path() / "models" / self.layer_folder / domain

    def get_path(
        self, name: Optional[str] = None, domain: Optional[str] = None
    ) -> Path:
        """Get full model path using either:
        - Cached values when initialized with path (name=None)
        - Explicit name/domain when initialized with layer
        """
        if name is None and hasattr(self, "_full_path"):
            return self._full_path
        if name is None:
            name = self.identifier if hasattr(self, "_identifier") else None
        return self.get_domain_path(domain) / f"{self.get_prefix(domain)}{name}"


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


def list_domain_models(base_path: ModelBasePath, domain: str):
    """List of existing models in the given layer/domain for this project"""
    yml_files = []
    for path in base_path.get_domain_path(domain).rglob("*.yml"):
        yml_files.append(str(path.stem))

    return yml_files
