import typer

from dbtwiz import admin, commands, model, source
from dbtwiz.helpers.logger import error

app = typer.Typer(
    context_settings={"help_option_names": ["-h", "--help"]},
    # add_completion=False,
)


# Add model commands as subcommands of 'model'
app.add_typer(model.app, name="model", help="Commands for a dbt model")

# Add source commands as subcommands of 'source'
app.add_typer(source.app, name="source", help="Commands for a dbt source")

# Add general commands
app.add_typer(commands.app)

# Add admin commands as subcommands of 'admin'
app.add_typer(admin.app, name="admin", help="Administrative commands")


# if __name__ == "__main__":
def main():
    """The main function for dbtwiz."""
    try:
        app()
    except commands.InvalidArgumentsError as err:
        error(f"ERROR: Invalid arguments - {err}")
        exit(1)
