from typing import List

from dbtwiz.logging import error
from dbtwiz.style import custom_style

def input_text(question, allow_blank=False, validate=None) -> str:
    """Ask user to input a text value"""
    from questionary import text # Lazy import for improved performance
    while True:
        value = text(
            f"{question}:",
            style=custom_style(),
            validate=validate
        ).unsafe_ask()
        if value or allow_blank:
            return value


def select_from_list(question, items, allow_none=False) -> (str | None):
    """Select item from list"""
    from questionary import select # Lazy import for improved performance
    na_selection = {"name": "n/a", "description": "Not relevant for this model"}
    default = None
    if allow_none:
        items.insert(0, na_selection)
        default = na_selection
    choice = select(
        f"{question}:",
        choices=items,
        use_shortcuts=True,
        style=custom_style(),
        default=default
    ).unsafe_ask()
    if choice == "n/a":
        return None
    return choice


def multiselect_from_list(question, items, allow_none=False) -> List[str]:
    """Select item from list"""
    from questionary import checkbox # Lazy import for improved performance
    validate = lambda sel: (len(sel) > 0) or "You must select at least one item"
    na_selection = {"name": "n/a", "description": "Not relevant for this model"}
    default = None
    if allow_none:
        items.insert(0, na_selection)
        default = na_selection
        validate = lambda sel: (
            len(sel) > 0 and
            (not ("n/a" in sel and len(sel) > 1))
        ) or "You must select at least one item, 'n/a' cannot be selected along with other options."
    choices = checkbox(
        f"{question}:",
        choices=items,
        validate=validate,
        style=custom_style(),
        default=default
    ).unsafe_ask()
    if choices == ["n/a"]:
        return None
    return choices


def autocomplete_from_list(question, items, must_exist=True, allow_blank=False) -> (str | None):
    """Select item from list with autocomplete and custom input"""
    from questionary import autocomplete # Lazy import for improved performance
    while True:
        opts = {"match_middle": True, "style": custom_style()}
        if isinstance(items, dict):
            opts["meta_information"] = items
        choice = autocomplete(
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
    from questionary import confirm # Lazy import for improved performance
    return confirm(question, style=custom_style()).ask()
