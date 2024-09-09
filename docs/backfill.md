# `dbtwiz backfill`

The _backfill_ subcommand allows you to (re)run date-partitioned models in production for a
period spanning one or multiple days. It will spawn a Cloud Run job that will run `dbt` for
a configurable number of days in parallel.


## Required arguments

- `select`: Model selector passed to dbt
- `date_first`: First date in the backfill period [YYYY-mm-dd]
- `date_last`: Last date in the backfill period (inclusive) [YYYY-mm-dd]


## Options

### `--parallelism (-p) INTEGER`

Number of days over which to run dbt in parallel through Cloud Run. For models that needs
serial execution, set this to `1`.

### `--full-refresh (-f)`

Build the model with full refresh, which causes existing tables to be deleted and
recreated. Needed when schema has changed between runs.

**This should only be used when backfilling a single date, ie. when _date_first_ and
_date_last_ are the same.**


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
