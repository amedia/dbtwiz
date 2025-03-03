import questionary

from typing import List

from dbtwiz.logging import error
from dbtwiz.style import custom_style


def input_text(question, allow_blank=False) -> str:
    """Ask user to input a text value"""
    while True:
        value = questionary.text(
            f"{question}:",
            style=custom_style()
        ).unsafe_ask()
        if value or allow_blank:
            return value


def select_from_list(question, items, allow_none=False) -> (str | None):
    """Select item from list"""
    choices = [i for i in items]
    if allow_none:
        choices.append("None")
    choice = questionary.select(
        f"{question}:",
        items,
        use_shortcuts=True,
        style=custom_style()
    ).unsafe_ask()
    if choice == "None":
        return None
    return choice


def multiselect_from_list(question, items) -> List[str]:
    """Select item from list"""
    choices = questionary.checkbox(
        f"{question}:",
        items,
        style=custom_style()
    ).unsafe_ask()
    return choices


def autocomplete_from_list(question, items, must_exist=True, allow_blank=False) -> (str | None):
    """Select item from list with autocomplete and custom input"""
    while True:
        if isinstance(items, dict):
            form = questionary.autocomplete(
                f"{question}:",
                items.keys(),
                meta_information=items,
                style=custom_style())
        else:
            form = questionary.autocomplete(
                f"{question}:",
                items,
                style=custom_style())
        choice = form.unsafe_ask()
        if (choice is None or choice == "") and allow_blank:
            return None
        if choice is not None and not must_exist:
            return choice
        if choice is not None and choice in items.keys():\
           return choice
        error(f"Choice {choice} is not in the list of allowed values.")
