# `dbtwiz backfill`

The _backfill_ subcommand allows you to (re)run date-partitioned models in production for a
period spanning one or multiple days. It will spawn a Cloud Run job that will run `dbt` for
a configurable number of days in parallel.

## Required arguments

- `select`: Model selector passed to dbt
- `date_first`: Start of backfill period [YYYY-mm-dd]
- `date_last`: End of backfill period (inclusive) [YYYY-mm-dd]

## Options

### `--full-refresh`, `-f`

Build the model with full refresh, which causes existing tables to be deleted and recreated. Needed when schema has changed between runs. **This should only be used when backfilling a single date, ie. when _date_first_ and _date_last_ are the same.**

### `--parallelism`, `-p`

Number of tasks to run in parallel. Set to 1 for serial processing, useful for models that depend on their own past data where the processing order is important.

### `--status`, `-s`

Open job status page in browser after starting execution

### `--verbose`, `-v`

Output more info about what is going on

## Examples

Example of the basic use-case:
```shell
$ dbtwiz backfill mymodel 2024-01-01 2024-01-31
```

Another example including downstream dependencies and serial execution (needed for models that
depends on previous partitions of their own data, for example):
```shell
$ dbtwiz backfill -p1 mymodel+ 2024-01-01 2024-01-15
```

After the job has been set up and passed on to Cloud Run, a status page should automatically
be opened in your browser so you can track progress.
