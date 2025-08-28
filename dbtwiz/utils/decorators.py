"""Decorators for dbtwiz package."""


def examples(*example_commands):
    """Custom decorator to add examples for typer commands."""

    def decorator(func):
        if not hasattr(func, "_command_examples"):
            func._command_examples = []
        func._command_examples.extend(example_commands)
        return func

    return decorator


def description(*description_commands):
    """Custom decorator to add extended description for typer commands."""

    def decorator(func):
        if not hasattr(func, "_command_description"):
            func._command_description = []
        func._command_description.extend(description_commands)
        return func

    return decorator
