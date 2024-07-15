from enum import Enum


class Target(str, Enum):
    dev = "dev"
    build = "build"
    prod = "prod"
    prod_ci = "prod-ci"
