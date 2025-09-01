import functools
from typing import Dict


class Theme:
    """
    Class holding color themes for model information output.

    A theme consists of a set of attributes with names indicating
    what information they are used to highlight.

    The attribute values are integers representing one of 256 distinct color
    values according to the ANSI standard. For a complete list, see:
    https://en.wikipedia.org/wiki/ANSI_escape_code#8-bit
    """

    # ============================================================================
    # CLASS CONSTANTS
    # ============================================================================
    COLOR_ATTRIBUTES = dict(
        name="Model name",
        path="Model file path",
        tags="Model tag list",
        group="Model group list",
        materialized="Model materialization type",
        owner="Model owner",
        policy="Model access policy",
        dep_stg="Model dependencies (staging)",
        dep_int="Model dependencies (intermediate)",
        dep_mart="Model dependencies (mart)",
        description="Model description",
        deprecated="Model description for deprecated models",
    )

    def __init__(self, **colors: int) -> None:
        """Construct a new theme with the given color attributes.

        Args:
            **colors: Color values as keyword arguments

        Raises:
            AttributeError: If the color keys don't match expected attributes
        """
        if colors.keys() != Theme.COLOR_ATTRIBUTES.keys():
            raise AttributeError(f"Bad color list: {colors.keys}")
        self._colors: Dict[str, int] = colors
        for key, value in colors.items():
            self.__setattr__(key, value)

    # ============================================================================
    # PUBLIC METHODS - Instance Methods
    # ============================================================================

    def color(self, name: str) -> int:
        """Get value of color with the given name.

        Args:
            name: Name of the color attribute

        Returns:
            Integer color value

        Raises:
            AttributeError: If the color name is invalid
        """
        try:
            return self._colors[name]
        except KeyError:
            raise AttributeError(f"Invalid color attribute '{name}'")

    def description(self, color: str) -> str:
        """Get description of the given color attribute.

        Args:
            name: Name of the color attribute

        Returns:
            Description string for the color attribute

        Raises:
            AttributeError: If the color name is invalid
        """
        try:
            return self.__class__.COLOR_ATTRIBUTES[color]
        except KeyError:
            raise AttributeError(f"Invalid color attribute '{color}'")

    # ============================================================================
    # PUBLIC METHODS - Class Methods
    # ============================================================================

    @classmethod
    def light(cls) -> "Theme":
        """Instantiate a light color theme.

        Returns:
            Theme instance with light color scheme
        """
        return Theme(
            name=30,
            path=27,
            tags=28,
            group=94,
            materialized=54,
            owner=136,
            policy=136,
            dep_stg=34,
            dep_int=24,
            dep_mart=20,
            description=102,
            deprecated=124,
        )

    @classmethod
    def dark(cls) -> "Theme":
        """Instantiate a dark color theme.

        Returns:
            Theme instance with dark color scheme
        """
        return Theme(
            name=115,
            path=147,
            tags=106,
            group=178,
            materialized=212,
            owner=208,
            policy=208,
            dep_stg=118,
            dep_int=123,
            dep_mart=75,
            description=144,
            deprecated=196,
        )

    @classmethod
    @functools.cache
    def by_name(cls, name: str) -> "Theme":
        """Instantiate theme by name.

        Args:
            name: Name of the theme to instantiate

        Returns:
            Theme instance with the specified color scheme

        Raises:
            AttributeError: If no theme exists with the given name
        """
        if name == "light":
            return cls.light()
        if name == "dark":
            return cls.dark()
        raise AttributeError(f"No theme named '{name}'")
