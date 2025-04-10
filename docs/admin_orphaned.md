# `dbtwiz admin orphaned`

List or delete orphaned materializations in the data warehouse

It will identify any tables/views created originally by dbt that are now outdated.
This is identified by comparing to the related manifest (dev or prod).

If using the list option, then it will only list the tables that are no longer present in the manifest.
If not then it will also enable selection of which tables to delete.

## Options

### `--target`, `-t`

Target

### `--list-only`, `-l`

List orphaned materializations without deleting anything

### `--force-delete`, `-f`

Delete orphaned materializations without asking (dev target only)
