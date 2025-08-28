"""Decorators for dbtwiz package."""

from typing import Any, Callable, TypeVar

F = TypeVar("F", bound=Callable[..., Any])


def examples(*example_commands: str) -> Callable[[F], F]:
    """Custom decorator to add examples for typer commands.

    Args:
        *example_commands: Variable number of example command strings

    Returns:
        Decorator function that adds examples to the command
    """

    def decorator(func: F) -> F:
        if not hasattr(func, "_command_examples"):
            func._command_examples = []
        func._command_examples.extend(example_commands)
        return func

    return decorator


def description(*description_commands: str) -> Callable[[F], F]:
    """Custom decorator to add extended description for typer commands.

    Args:
        *description_commands: Variable number of description strings

    Returns:
        Decorator function that adds descriptions to the command
    """

    def decorator(func: F) -> F:
        if not hasattr(func, "_command_description"):
            func._command_description = []
        func._command_description.extend(description_commands)
        return func

    return decorator
