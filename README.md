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

[comment]: <> (START COMMAND DOCS)

- `model` - Commands for a dbt model
  - [`create`](docs/model_create.md) - Create new dbt model.
  - [`fix`](docs/model_fix.md) - Run sqlfmt and sqlfix for staged and/or defined sql files.
  - [`from-sql`](docs/model_from_sql.md) - Convert a sql file to a dbt model by replacing table references with source and ref.
  - [`inspect`](docs/model_inspect.md) - Output information about a given model.
  - [`lint`](docs/model_lint.md) - Run sqlfmt --diff and sqlfluff lint for staged and/or defined sql files.
  - [`move`](docs/model_move.md) - Moves a model by copying to a new location with a new name,
and/or by updating the references to the model by other dbt models.
- `source` - Commands for a dbt source
  - [`create`](docs/source_create.md) - Create new dbt source
- [`build`](docs/build.md) - Build one or more dbt models, using interactive selection with fuzzy-matching,
unless an exact model name is passed.
- [`test`](docs/test.md) - Test dbt models
- [`manifest`](docs/manifest.md) - Update dev and production manifests for fast lookup
- [`backfill`](docs/backfill.md) - The _backfill_ subcommand allows you to (re)run date-partitioned models in production for a
period spanning one or multiple days. It will spawn a Cloud Run job that will run `dbt` for
a configurable number of days in parallel.
- [`config`](docs/config.md) - Update user configuration setting
- `admin` - Administrative commands
  - [`cleandev`](docs/admin_cleandev.md) - Delete all materializations in the dbt development dataset
  - [`orphaned`](docs/admin_orphaned.md) - List or delete orphaned materializations in the data warehouse
  - [`partition-expiry`](docs/admin_partition_expiry.md) - Checks for mismatched partition expiry and allows updating to correct.

[comment]: <> (END COMMAND DOCS)

## Configuration

### Project config
Depending on the specific subcommand, there are some configuration settings defined in a `pyproject.toml` file that the tool will look for.

The tool will give you a warning when you run a commmand that needs one of the config elements should it be missing, so you don't need to add them all before they become relevant.

```
[tool.dbtwiz.project]
# Config for bucket containing dbt manifest.json at the top level
bucket_state_project = ""         # Project name for bucket
bucket_state_identifier = ""      # Bucket name

# Config for service account used for backfill and cleanup of orphaned models in prod
service_account_project = ""      # Project name for where service account actions are run
service_account_identifier = ""   # Name of service account
service_account_region = ""       # Region for where service account actions are run

# Config for user actions
user_project = ""                 # Project name for where user queries are run
user_auth_verified_domains = []   # Which domains to check when identifying whether user is already authenticated

# Config for docker image used for backfill
docker_image_url_dbt = ""         # Url for docker image
docker_image_profiles_path = ""   # Path to profiles dir in docker image
docker_image_manifest_path = ""   # Path to manifest in docker image
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
