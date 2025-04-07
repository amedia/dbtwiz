# `dbtwiz build`

Build one or more dbt models, using interactive selection with fuzzy-matching,
unless an exact model name is passed.

## Options

### `--target`, `-t`

Build target. Only dev is supported.

As a model developer, you should never have to use this option.
If you need to rebuild models in production, use the [backfill](backfill.md) command.

### `--select`, `-s`

Model selector. If an existing model matches the given selector string exactly,
it will build with no further interaction. Otherwise an interactive selection
list will show all models which partially matches using fuzzy match to allow
you to refine your search.

### `--date`

Date in `YYYY-mm-dd` format.
For partitioned models, this option sets the date to be passed as `data_interval_start`
variable and will be picked up by the `start_date()` macro by the models.

### `--use-task-index`

This option is only relevant for backfilling, and is set by Cloud Run to
offset the date for partitioned models relative to the start date.

### `--save-state`

For production runs, this option will cause the resulting manifest to be copied over
to the state bucket after the build has successfully completed. This is only relevant
when running with target `prod`, and should not be used elsewhere.

The state bucket is set in the project's _pyproject.toml_ file in the
section `[tool.dbtwiz.project]` and the setting `bucket_state_identifier`.

### `--full-refresh`, `-f`

Build the model with full refresh, which causes existing tables to be deleted and
recreated. Needed when schema has changed between runs.

### `--upstream`, `-u`

Also build upstream models on which the selected model(s) are dependent.
This will prepend a '+' to your chosen models when passing them on to _dbt_.

### `--downstream`, `-d`

Also build downstream models that are directly or indirectly depdendent on the selected model(s).
This will append a '+' to your chosen models when passing them on to _dbt_.

### `--work`, `-w`

When used, this option causes interactive selection to include only models that
have *staged* local modifications according to `git status`.

### `--repeat-last`, `-l`

When you build with _dbtwiz_, it will store a list of selected models in the
file `.dbtwiz/last_select.json` in the current project.

Pass this option to rebuild the same models that you most recently built.
