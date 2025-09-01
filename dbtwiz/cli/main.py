import typer

from .. import admin, commands, model, source
from ..utils.logger import error

app = typer.Typer(
    name="dbtwiz",
    help="A CLI helper tool for dbt development and administration in GCP using BigQuery",
    context_settings={"help_option_names": ["-h", "--help"]},
    # add_completion=False,
)


# Add model commands as subcommands of 'model'
app.add_typer(model.app, name="model", help="Create, validate, and manage dbt models")

# Add source commands as subcommands of 'source'
app.add_typer(source.app, name="source", help="Create and manage dbt sources")

# Add general commands
app.add_typer(commands.app, help="Build and test dbt models")

# Add admin commands as subcommands of 'admin'
app.add_typer(
    admin.app, name="admin", help="Production backfilling and administrative tasks"
)


# if __name__ == "__main__":
def main() -> None:
    """The main function for dbtwiz.

    This function serves as the entry point for the CLI application.
    It catches InvalidArgumentsError and displays appropriate error messages.
    """
    try:
        app()
    except commands.InvalidArgumentsError as err:
        error(f"ERROR: Invalid arguments - {err}")
        exit(1)
