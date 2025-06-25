# `dbtwiz admin cleandev`

Delete all materializations in the dbt development dataset

Unless overriden, it will default to looking for target called `dev` in profiles.yml.
The user will be prompted before any tables are deleted.

By using defer, it is good practice to routinely clean the dbt dev dataset to ensure up to date production tables are used.

## Options

### `--target`, `-t`

Target

### `--force-delete`, `-f`

Delete without asking for confirmation first
