# dbtwiz
Python package with CLI helper tool for dbt

## Installation

```
pip install git+https://github.com/amedia/dbtwiz
```

## Development

```
# where you keep locally checked out repos:
git clone git@github.com:amedia/dbtwiz

# inside the virtual environment of your dbt project:
pip install -e <local-path-to-dbtwiz-repository>
```

## Configuration

The default configuration of _dbtwiz_ will be installed the first time you run it, but you
may want to adjust some settings from the get-go to fit your environment.

### Dark mode
If you're using a dark background colour in your terminal, you should configure _dbtwiz_ to
use bright colours for highlighting in previews and elsewhere to make the text more readable.

Run the following command to switch from default light mode to dark mode:
```shell
$ dbtwiz config theme dark
```

### Preview formatter

By default, _dbtwiz_ uses the command _fmt_ tool to format text in the preview window when
selecting models interactively. Under macOS, the _fmt_ tool won't handle ANSI escape codes,
and unless you have the GNU coreutils version of _fmt_ you will get garbage characters in the
preview window, and should switch to the simple _cat_ command for formatting instead:
```shell
$ dbtwiz config model_info:formatter "cat -s"
```

## Subcommands

- `model`: Commands for a dbt model
    - `create`: Create new dbt model
    - `inspect`: Output information about a given model
    - `from-sql`: Convert a sql file to a dbt model by replacing table references with source and ref
    - `move`: Moves a model by copying to a new location with a new name, and/or by updating the references to the model by other dbt models.
- `source`: Commands for a dbt source
    - `create`: Create new dbt source
- [build](docs/build.md): Build dbt models
- `test`: Test dbt models
- `sqlfix`: Run sqlfmt-fix and sqlfluff-fix on staged changes
- `manifest`: Update dev and production manifests for fast lookup
- [backfill](docs/backfill.md): Backfill dbt models by generating job spec and execute through Cloud Run
- `freshness`: Run source freshness tests
- `config`: Update configuration setting
- `admin`: Administrative commands
    - `orphaned`: List or delete orphaned materializations in the data warehouse
    - `cleandev`: Delete all materializations in the dbt development dataset
    - `partition-expiry`: Checks for mismatched partition expiry and allows updating to correct
