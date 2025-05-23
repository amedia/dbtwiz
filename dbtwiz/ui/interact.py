import re
from typing import List

from dbtwiz.helpers.logger import error, fatal

from .style import custom_style


def name_validator():
    """Returns the default validator for names."""
    return (
        lambda string: (re.match(r"^[a-z][a-z0-9_]*[a-z0-9]$", string) is not None)
        or "The value can only contain lowercase, digits and underscores, must start with a character and not end with underscore"
    )


def dataset_name_validator():
    """Returns the validator for dataset names."""
    return (
        lambda string: "The value can only contain lowercase, digits, and underscores, and must start with a letter. INFORMATION_SCHEMA is allowed."
        if string != "INFORMATION_SCHEMA"
        and not re.match(r"^[a-z][a-z0-9_]*[a-z0-9]$", string)
        else True
    )


def table_name_validator(dataset_name):
    """Returns the validator for table names."""
    if dataset_name == "INFORMATION_SCHEMA":
        return (
            lambda string: re.match(r"^[A-Z][A-Z0-9_]*[A-Z0-9]$", string) is not None
            or "The table can only contain uppercase, digits and underscores, must start with a character and not end with underscore."
        )
    else:
        return (
            lambda string: re.match(r"^[a-z][a-z0-9_]*[a-z0-9]$", string) is not None
            or "The value can only contain lowercase letters, digits, and underscores, starting with a lowercase letter and not ending with an underscore."
        )


def description_validator():
    """Returns the default validator for descriptions."""
    return (
        lambda string: (re.match(r"^\S+", string) is not None)
        or "The description must not start with a space"
    )


def input_text(question, allow_blank=False, validate=None) -> str:
    """Ask user to input a text value"""
    from questionary import text  # Lazy import for improved performance

    while True:
        value = text(
            f"{question}:", style=custom_style(), validate=validate
        ).unsafe_ask()
        if value or allow_blank:
            return value


def select_from_list(
    question, items, allow_none=False, use_shortcuts=True
) -> str | None:
    """Select item from list"""
    from questionary import select  # Lazy import for improved performance

    na_selection = {"name": "n/a", "description": "Not relevant"}
    default = None
    if allow_none:
        items.insert(0, na_selection)
        default = na_selection
    choice = select(
        f"{question}:",
        choices=items,
        use_shortcuts=use_shortcuts,
        style=custom_style(),
        default=default,
    ).unsafe_ask()
    if choice == "n/a":
        return None
    return choice


def validate_selection(selection) -> str | bool:
    """Validate that the selection contains at least one item."""
    if len(selection) == 0:
        return "You must select at least one item"
    return True


def validate_selection_with_na(selection) -> str | bool:
    """
    Validate that the selection contains at least one item, and that 'n/a'
    is not selected along with other options.
    """
    if len(selection) == 0:
        return "You must select at least one item"
    if "n/a" in selection and len(selection) > 1:
        return "'n/a' cannot be selected along with other options"
    return True


def multiselect_from_list(question, items, allow_none=False) -> List[str]:
    """Select item from list"""
    from questionary import checkbox  # Lazy import for improved performance

    na_selection = {"name": "n/a", "description": "Not relevant"}
    default = None
    if allow_none:
        items.insert(0, na_selection)
        default = na_selection
    choices = checkbox(
        f"{question}:",
        choices=items,
        validate=validate_selection_with_na if allow_none else validate_selection,
        style=custom_style(),
        default=default,
    ).unsafe_ask()
    if choices == ["n/a"]:
        return None
    return choices


def autocomplete_from_list(
    question, items, must_exist=True, allow_blank=False, validate=None
) -> str | None:
    """Select item from list with autocomplete and custom input"""
    from questionary import autocomplete  # Lazy import for improved performance

    while True:
        opts = {"match_middle": True, "style": custom_style()}
        if isinstance(items, dict):
            opts["meta_information"] = items
        if validate:
            opts["validate"] = validate
        choice = autocomplete(
            f"{question}: (start typing, TAB for autocomplete)",
            items,
            **opts,
        ).unsafe_ask()
        if choice is None or choice == "":
            if allow_blank:
                return None
            else:
                error("A non-empty choice is required.")
                continue
        if not must_exist:
            return choice
        if choice in items:
            return choice
        error(f"Choice {choice} is not in the list of allowed values.")


def confirm(question: str) -> bool:
    """Ask user for confirmation. Exits completely on cancellation."""
    from questionary import confirm as questionary_confirm  # Avoid name collision

    try:
        answer = questionary_confirm(question, style=custom_style()).unsafe_ask()
        return answer
    except KeyboardInterrupt:
        fatal("Cancelling")
    except Exception as e:
        fatal(f"\nError: {str(e)}")
