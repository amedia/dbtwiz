import typer
from rich.console import Console
from rich.panel import Panel

from dbtwiz.config.user import user_config

log_console, error_console = Console(), Console(stderr=True)


def debug(message: str):
    """Log a debug message when enabled"""
    if user_config().log_debug:
        log_console.print(message, style="blue")


def info(message: str, style: str = "green"):
    """Log an info message"""
    log_console.print(message, style=style)


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


def notice(message: str):
    """Log a notice"""
    log_console.print(
        Panel(
            message,
            title="[bold green]Notice[/]",
            border_style="green",
            title_align="left",
        )
    )


def status(message: str, status_text: str = "", style: str = "yellow"):
    """Log a status message that can be updated"""
    if status_text:
        log_console.print(f"{message}: [bold {style}]{status_text}[/]")
    else:
        log_console.print(f"{message}: ...", end="\r")
