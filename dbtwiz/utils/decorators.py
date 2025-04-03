def examples(*example_commands):
    """Custom decorator to add examples for typer commands."""

    def decorator(func):
        if not hasattr(func, "_command_examples"):
            func._command_examples = []
        func._command_examples.extend(example_commands)
        return func

    return decorator
