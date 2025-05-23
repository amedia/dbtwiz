from pathlib import Path
from typing import Optional, Union

from dbtwiz.config.project import project_path


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
        self.path = path.parent / path.stem

        parts = self.path.parts
        try:
            models_pos = parts.index("models")
            if len(parts) > models_pos + 2:
                # Extract folder structure
                layer_folder = parts[models_pos + 1]
                self._domain = parts[models_pos + 2]
                self._model_name = path.name

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
