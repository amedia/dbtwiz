# `dbtwiz admin cleandev`

Delete all materializations in the dbt development dataset

By using defer, it is good practice to routinely clean the dbt dev dataset to ensure up to date production tables are used.

## Options

### `--force-delete`, `-f`

Delete without asking for confirmation first
