from dbtwiz.logging import info, error
from dbtwiz.model_step import ModelStep
from dbtwiz.model_group import ModelGroup
from dbtwiz.style import custom_style

import questionary


def generate_model(step=None, group=None):
    """Generate new dbt model"""

    # Ask for step (staging, intermediate, marts)
    if step is None:
        step = questionary.select(
            "Which step do you wish to generate a model for:",
            list(ModelStep.__members__),
            use_shortcuts=True,
            style=custom_style()
        ).ask()
    else:
        step = step.value

    # Ask for model group
    model_group = ModelGroup()
    if group is None:
        group = questionary.autocomplete(
            "Which model group does your new model belong to:",
            model_group.names(),
            meta_information=model_group.descriptions(),
            style=custom_style(),
        ).ask()
    if group not in model_group.names():
        error(f"Model group '{group}' is not in the defined groups: {model_group.names()}")
        return

    info(f"Generating model:")
    info(f"  Step: {step}")
    info(f"  Group: {group}")
