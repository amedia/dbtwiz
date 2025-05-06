import functools


class Theme:
    """
    Class holding color themes for model information output.

    A theme consists of a set of attributes with names indicating
    what information they are used to highlight.

    The attribute values are integers representing one of 256 distinct color
    values according to the ANSI standard. For a complete list, see:
    https://en.wikipedia.org/wiki/ANSI_escape_code#8-bit
    """

    # Color attributes and their descriptions
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


    def __init__(self, **colors):
        """Construct a new theme with the given color attributes"""
        if colors.keys() != Theme.COLOR_ATTRIBUTES.keys():
            raise AttributeError(f"Bad color list: {colors.keys}")
        self._colors = colors
        for key, value in colors.items():
            self.__setattr__(key, value)


    def color(self, name: str):
        """Get value of color with the given name"""
        try:
            self._colors[name]
        except KeyError:
            raise AttributeError(f"Invalid color attribute '{name}'")


    def description(self, color: str):
        """Get description of the given color attribute"""
        try:
            return self.__class__.COLOR_ATTRIBUTES[color]
        except KeyError:
            raise AttributeError(f"Invalid color attribute '{color}'")


    @classmethod
    def light(cls):
        """Instantiate a light color theme"""
        return Theme(name=30, path=27, tags=28, group=94, materialized=54,
                     owner=136, policy=136, dep_stg=34, dep_int=24, dep_mart=20,
                     description=102, deprecated=124)

    @classmethod
    def dark(cls):
        """Instantiate a dark color theme"""
        return Theme(name=115, path=147, tags=106, group=178, materialized=212,
                     owner=208, policy=208, dep_stg=118, dep_int=123, dep_mart=75,
                     description=144, deprecated=196)

    @classmethod
    @functools.cache
    def by_name(cls, name: str):
        """Instantiate theme by name"""
        if name == "light":
            return cls.light()
        if name == "dark":
            return cls.dark()
        raise AttributeError(f"No theme named '{name}'")
