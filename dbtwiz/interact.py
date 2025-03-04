import questionary

import re
from typing import List

from dbtwiz.logging import error
from dbtwiz.style import custom_style


def input_text(question, allow_blank=False, pattern=None) -> str:
    """Ask user to input a text value"""
    while True:
        opts = {}
        if pattern:
            opts["validate"] = lambda string: re.match(pattern, string) is not None
        value = questionary.text(
            f"{question}:",
            style=custom_style(),
            **opts,
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


def multiselect_from_list(question, items, allow_none=False) -> List[str]:
    """Select item from list"""
    choice_list = [i for i in items]
    if allow_none:
        choice_list.append("None")
    choices = questionary.checkbox(
        f"{question}:",
        choice_list,
        validate=lambda sel: len(sel) > 0,  # Must choose at least one
        style=custom_style()
    ).unsafe_ask()
    return choices


def autocomplete_from_list(question, items, must_exist=True, allow_blank=False) -> (str | None):
    """Select item from list with autocomplete and custom input"""
    while True:
        opts = {"match_middle": True, "style": custom_style()}
        if isinstance(items, dict):
            opts["meta_information"] = items
        choice = questionary.autocomplete(
            f"{question}: (start typing, TAB for autocomplete)", items, **opts
        ).unsafe_ask()
        if choice is None or choice == "":
            if allow_blank:
                return None
            else:
                error(f"A non-empty choice is required.")
                continue
        if not must_exist:
            return choice
        if choice in items.keys():\
           return choice
        error(f"Choice {choice} is not in the list of allowed values.")


def confirm(question: str):
    """Ask user for confirmation"""
    return questionary.confirm(question, style=custom_style()).ask()
