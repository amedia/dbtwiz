import sys

from rich.console import Console


log_console, error_console = Console(), Console(stderr=True)


def debug(message: str):
    log_console.print(message, style="blue")

def info(message: str):
    log_console.print(message, style="green")

def warn(message: str):
    log_console.print(message, style="color(166)")

def error(message: str):
    error_console.print(message, style="red")

def fatal(message: str, exit_code=1):
    error_console.print(message, style="red")
    sys.exit(exit_code)
