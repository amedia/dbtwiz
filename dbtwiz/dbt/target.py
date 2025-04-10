from enum import Enum


class Target(str, Enum):
    """Enumeration of target environments."""

    dev = "dev"
    build = "build"
    prod = "prod"
    prod_ci = "prod-ci"
