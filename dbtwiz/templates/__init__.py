from pathlib import Path


def path_to_template(name: str) -> Path:
    """Get path to templates within the dbtwiz package"""
    return Path(__file__).parent / f"{name}.tpl"
