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
  - [`inspect`](docs/model_inspect.md) - Output information about a given model.
  - [`lint`](docs/model_lint.md) - Run sqlfmt --diff and sqlfluff lint for staged and/or defined sql files.
  - [`move`](docs/model_move.md) - Moves a model by copying to a new location with a new name,
and/or by updating the references to the model by other dbt models.
  - [`validate`](docs/model_validate.md) - Validates the yml and sql files for a model.
- `source` - Commands for a dbt source
  - [`create`](docs/source_create.md) - Create new dbt source
- [`build`](docs/build.md) - Build one or more dbt models, using interactive selection with fuzzy-matching,
unless an exact model name is passed.
- [`test`](docs/test.md) - Test dbt models
- [`manifest`](docs/manifest.md) - Update dev and production manifests for fast lookup
- [`backfill`](docs/backfill.md) - The _backfill_ subcommand allows you to (re)run date-partitioned models in production for a
period spanning one or multiple days. It will spawn a Cloud Run job that will run `dbt` for
a configurable number of days in parallel.
- `admin` - Administrative commands
  - [`cleandev`](docs/admin_cleandev.md) - Delete all materializations in the dbt development dataset
  - [`orphaned`](docs/admin_orphaned.md) - List or delete orphaned materializations in the data warehouse
  - [`partition-expiry`](docs/admin_partition_expiry.md) - Checks for mismatched partition expiry and allows updating to correct.

[comment]: <> (END COMMAND DOCS)

## Configuration

### Project config
Depending on the specific subcommand, there are some configuration settings defined in a `pyproject.toml` file that the tool will look for.

The tool will give you a warning when you run a commmand that needs one of the config elements should it be missing, so you don't need to add them all before they become relevant.

```toml
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

The config settings are stored in a file `config.toml` in the `dbtwiz` folder within
your user's app settings directory. In a GitHub Codespace or in a local Linux environment
the file is located at `~/.config/dbtwiz/config.toml`, while in a local Windows environment
it's located at `%appdata%/dbtwiz/config.toml`. TODO: Where is it located in MacOS?

If dbtwiz looks up a user config file and it doesn't exist, it will be created with
the following default settings:

```toml
# When true, check for existing GCP auth token, and ask for
# automatic reauthentication if needed.
auth_check = true

# Command for opening model source files in editor
editor = "code"

# Enable debug logging of some internal dbtwiz operations. You won't
# need this unless you're working on or helping troubleshoot dbtwiz.
log_debug = false

# Command for showing prerendered model info files in the interactive
# fzf-based selector. The default should work in GitHub Codespaces,
# on Linux or on MacOS with GNU coreutils installed.
# Here are some alternatives:
# - MacOS without GNU coreutils: "cat -s"
# - Windows using PowerShell: "cat" or "Get-Content"
# - Windows using cmd.exe in Terminal: "type"
model_formatter = "fmt -s"

# Set to "light" to use a color scheme suitable for a light background,
# or to "dark" for better contrasts against a dark background.
theme = "light"
```

## Development

```
# where you keep locally checked out repos:
git clone git@github.com:amedia/dbtwiz

# inside the virtual environment of your dbt project:
pip install -e <local-path-to-dbtwiz-repository>
```
