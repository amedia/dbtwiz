# dbtwiz
Python package with CLI helper tool for dbt in GCP using BigQuery.
Although some functions are platform independent, the majority assume GCP and BigQuery is used.

## Installation

```
pip install git+https://github.com/amedia/dbtwiz
```

## Subcommands

These are the available subcommands for `dbtwiz`.
You can also run `dbtwiz --help`/`dbtwiz -h`, which will list the commands with more details.

[comment]: <> (START ACCESS CONFIG)

- `model` - Commands for a dbt model
  - [`create`](docs/model_create.md) - Create new dbt model.
  - [`inspect`](docs/model_inspect.md) - Output information about a given model.
  - [`from_sql`](docs/model_from_sql.md) - Convert a sql file to a dbt model by replacing table references with source and ref.
  - [`move`](docs/model_move.md) - Moves a model by copying to a new location with a new name,
and/or by updating the references to the model by other dbt models.
- `source` - Commands for a dbt source
  - [`create`](docs/source_create.md) - Create new dbt source
- [`build`](docs/build.md) - Build one or more dbt models, using interactive selection with fuzzy-matching,
unless an exact model name is passed.
- [`test`](docs/test.md) - Test dbt models
- [`sqlfix`](docs/sqlfix.md) - Run sqlfmt-fix and sqlfluff-fix on staged changes
- [`manifest`](docs/manifest.md) - Update dev and production manifests for fast lookup
- [`backfill`](docs/backfill.md) - The _backfill_ subcommand allows you to (re)run date-partitioned models in production for a
period spanning one or multiple days. It will spawn a Cloud Run job that will run `dbt` for
a configurable number of days in parallel.
- [`freshness`](docs/freshness.md) - Run source freshness tests
- [`config`](docs/config.md) - Update configuration setting
- `admin` - Administrative commands
  - [`orphaned`](docs/admin_orphaned.md) - List or delete orphaned materializations in the data warehouse
  - [`cleandev`](docs/admin_cleandev.md) - Delete all materializations in the dbt development dataset
  - [`partition_expiry`](docs/admin_partition_expiry.md) - Checks for mismatched partition expiry and allows updating to correct.

[comment]: <> (END ACCESS CONFIG)

## Configuration

### Project config
Depending on the specific subcommand, there are some configuration settings defined in a `pyproject.toml` file that the tool will look for.

The tool will give you a warning when you run a commmand that needs one of the config elements should it be missing, so you don't need to add them all before they become relevant.

```
[tool.dbtwiz.project]
bucket_state_project = ""
bucket_state_identifier = ""
service_account_project = ""
service_account_identifier = ""
service_account_region = ""
user_project = ""
user_auth_verified_domains = []
docker_image_url_dbt = ""
```

### User config
The default configuration of _dbtwiz_ will be installed the first time you run it, but you
may want to adjust some settings from the get-go to fit your environment.

#### Dark mode
If you're using a dark background colour in your terminal, you should configure _dbtwiz_ to
use bright colours for highlighting in previews and elsewhere to make the text more readable.

Run the following command to switch from default light mode to dark mode:
```shell
$ dbtwiz config theme dark
```

#### Preview formatter

By default, _dbtwiz_ uses the command _fmt_ tool to format text in the preview window when
selecting models interactively. Under macOS, the _fmt_ tool won't handle ANSI escape codes,
and unless you have the GNU coreutils version of _fmt_ you will get garbage characters in the
preview window, and should switch to the simple _cat_ command for formatting instead:
```shell
$ dbtwiz config model_info:formatter "cat -s"
```

## Development

```
# where you keep locally checked out repos:
git clone git@github.com:amedia/dbtwiz

# inside the virtual environment of your dbt project:
pip install -e <local-path-to-dbtwiz-repository>
```
