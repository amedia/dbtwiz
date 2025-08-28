"""Enhanced logging utilities for dbtwiz package."""

import traceback
from pathlib import Path
from typing import Any, Dict, Optional

import typer
from rich.console import Console
from rich.panel import Panel
from rich.traceback import install

from ..config.user import user_config

# Install rich traceback handler for better error display
install(show_locals=True)

log_console, error_console = Console(), Console(stderr=True)


def debug(message: str, context: Optional[Dict[str, Any]] = None) -> None:
    """Log a debug message when enabled.

    Args:
        message: The debug message to log
        context: Optional context dictionary with additional information
    """
    if user_config().log_debug:
        if context:
            message = f"{message} | {_format_context(context)}"
        log_console.print(f"[DEBUG] {message}", style="blue")


def info(
    message: str, style: str = "green", context: Optional[Dict[str, Any]] = None
) -> None:
    """Log an info message.

    Args:
        message: The info message to log
        style: Rich text style to apply to the message
        context: Optional context dictionary with additional information
    """
    if context:
        message = f"{message} | {_format_context(context)}"
    log_console.print(message, style=style)


def warn(message: str, context: Optional[Dict[str, Any]] = None) -> None:
    """Log a warning message.

    Args:
        message: The warning message to log
        context: Optional context dictionary with additional information
    """
    if context:
        message = f"{message} | {_format_context(context)}"
    log_console.print(
        Panel(
            message,
            title="[bold yellow]Warning[/]",
            border_style="yellow",
            title_align="left",
        )
    )


def error(
    message: str,
    context: Optional[Dict[str, Any]] = None,
    exception: Optional[Exception] = None,
) -> None:
    """Log an error message with optional context and exception details.

    Args:
        message: The error message to log
        context: Optional context dictionary with additional information
        exception: Optional exception object to include in the log
    """
    if context:
        message = f"{message} | {_format_context(context)}"

    if exception:
        message = (
            f"{message}\n\nException: {type(exception).__name__}: {str(exception)}"
        )
        if user_config().log_debug:
            message = f"{message}\n\nTraceback:\n{''.join(traceback.format_tb(exception.__traceback__))}"

    error_console.print(
        Panel(
            message, title="[bold red]Error[/]", border_style="red", title_align="left"
        )
    )


def fatal(
    message: str,
    exit_code: int = 1,
    context: Optional[Dict[str, Any]] = None,
    exception: Optional[Exception] = None,
) -> None:
    """Log an error message then quit the application.

    Args:
        message: The error message to log
        exit_code: Exit code to use when terminating
        context: Optional context dictionary with additional information
        exception: Optional exception object to include in the log

    Raises:
        typer.Exit: Always raised to terminate the application
    """
    error(message, context, exception)
    raise typer.Exit(code=exit_code)


def notice(message: str, context: Optional[Dict[str, Any]] = None) -> None:
    """Log a notice message.

    Args:
        message: The notice message to log
        context: Optional context dictionary with additional information
    """
    if context:
        message = f"{message} | {_format_context(context)}"
    log_console.print(
        Panel(
            message,
            title="[bold green]Notice[/]",
            border_style="green",
            title_align="left",
        )
    )


def status(
    message: str,
    status_text: str = "",
    style: str = "yellow",
    context: Optional[Dict[str, Any]] = None,
) -> None:
    """Log a status message that can be updated.

    Args:
        message: The status message to log
        status_text: Optional status text to display
        style: Rich text style to apply to the status text
        context: Optional context dictionary with additional information
    """
    if context:
        message = f"{message} | {_format_context(context)}"

    if status_text:
        log_console.print(f"{message}: [bold {style}]{status_text}[/]")
    else:
        log_console.print(f"{message}: ...", end="\r")


def _format_context(context: Dict[str, Any]) -> str:
    """Format context dictionary for logging.

    Args:
        context: Dictionary containing context information

    Returns:
        Formatted string representation of the context
    """
    if not context:
        return ""

    formatted_parts = []
    for key, value in context.items():
        if isinstance(value, Path):
            formatted_parts.append(f"{key}={value}")
        elif isinstance(value, (list, tuple)):
            formatted_parts.append(f"{key}={', '.join(map(str, value))}")
        else:
            formatted_parts.append(f"{key}={value}")

    return " | ".join(formatted_parts)


def log_function_call(
    func_name: str,
    args: Optional[tuple] = None,
    kwargs: Optional[dict] = None,
    context: Optional[Dict[str, Any]] = None,
) -> None:
    """Log function calls for debugging.

    Args:
        func_name: Name of the function being called
        args: Optional tuple of positional arguments
        kwargs: Optional dictionary of keyword arguments
        context: Optional context dictionary with additional information
    """
    if not user_config().log_debug:
        return

    call_info = f"Calling {func_name}"
    if args:
        call_info += f" with args: {args}"
    if kwargs:
        call_info += f" with kwargs: {kwargs}"

    debug(call_info, context)


def log_function_result(
    func_name: str,
    result: Optional[Any] = None,
    context: Optional[Dict[str, Any]] = None,
) -> None:
    """Log function results for debugging.

    Args:
        func_name: Name of the function that completed
        result: Optional result value from the function
        context: Optional context dictionary with additional information
    """
    if not user_config().log_debug:
        return

    result_info = f"{func_name} completed"
    if result is not None:
        result_info += f" with result: {result}"

    debug(result_info, context)
