from rich.console import Console
from rich.panel import Panel

import typer


log_console, error_console = Console(), Console(stderr=True)


def debug(message: str):
    """Log a debug message"""
    log_console.print(message, style="blue")


def info(message: str):
    """Log an info message"""
    log_console.print(message, style="green")


def warn(message: str):
    """Log a warning"""
    log_console.print(
        Panel(
            message,
            title="[bold yellow]Warning[/]",
            border_style="yellow",
            title_align="left",
        )
    )


def error(message: str):
    """Log an error message"""
    error_console.print(
        Panel(
            message, title="[bold red]Error[/]", border_style="red", title_align="left"
        )
    )


def fatal(message: str, exit_code=1):
    """Log an error message then quit the application"""
    error(message)
    raise typer.Exit(code=exit_code)
