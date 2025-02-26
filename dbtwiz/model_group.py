import yaml

from .config import project_path

class ModelGroup:

    MODEL_GROUPS_PATH = project_path() / "models" / "model_groups.yml"


    def __init__(self):
        with open(self.MODEL_GROUPS_PATH, "r") as f:
            data = yaml.safe_load(f)
        self.groups = data["groups"]


    def names(self):
        """Return a list of all group names"""
        return [group["name"] for group in self.groups]


    def descriptions(self):
        """Returns a dict with group names as keys and descriptions as values"""
        return dict([
            (group["name"], group["owner"]["description"])
            for group in self.groups
        ])
