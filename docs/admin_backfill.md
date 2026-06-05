# `dbtwiz admin backfill`

Backfill date-partitioned models in production for a specified date range.

Spawns Cloud Run jobs to process multiple dates in parallel with configurable batch sizes.
Use --retry to re-run only the failed tasks from the most recent execution.

## Required arguments

- `select`: Model selector passed to dbt
- `date_first`: Start of backfill period [YYYY-mm-dd]. Optional (and ignored) when --retry is set.
- `date_last`: End of backfill period (inclusive) [YYYY-mm-dd]. Defaults to date_first.

## Options

### `--batch-size`, `-b`

Number of dates to include in each batch. When used with --retry, subdivides each failed range to this size; if omitted on retry, the failed ranges are retried as-is.

### `--retry`

Retry only the failed tasks from the most recent execution of this backfill job (looked up by selector). When set, date arguments are ignored. Pass --batch-size to subdivide failed ranges before retrying.

### `--full-refresh`, `-f`

Build the model with full refresh, which causes existing tables to be deleted and recreated. Needed when schema has changed between runs. **This should only be used when backfilling a single date, ie. when _date_first_ and _date_last_ are the same.**

### `--parallelism`, `-p`

Number of tasks to run in parallel. Set to 1 for serial processing, useful for models that depend on their own past data where the processing order is important.

### `--status`, `-s`

Open job status page in browser after starting execution

### `--verbose`, `-v`

Output more info about what is going on

### `--dry-run`

Print the tasks that would be submitted without actually running the job. Works with and without --retry.

## Examples

Example of the basic use-case:
```shell
$ dbtwiz backfill mymodel 2024-01-01 2024-01-31
```

Another example including downstream dependencies and serial execution (needed for models that
depends on previous partitions of their own data, for example):
```shell
$ dbtwiz backfill mymodel+ 2024-01-01 2024-01-15 -p 1
```

After the job has been set up and passed on to Cloud Run, a status page should automatically
be opened in your browser so you can track progress.
