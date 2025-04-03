# `dbtwiz model create`

Create new dbt model

## Options

### `--quick`, `-q`

Skip non-essential questions to quickly get started

### `--layer`, `-l`

The layer for the model

### `--source`, `-s`

Which source a staging model should be built on top of

### `--domain`, `-d`

The domain the model should belong to

### `--name`, `-n`

The name of the model

### `--description`, `-ds`

A short description for the model

### `--group`, `-g`

Which group the model should belong to

### `--access`, `-a`

What the access level should be for the model

### `--materialization`, `-m`

How the model should be materialized

### `--expiration`, `-e`

The data expiration policy for an incremental model

### `--team`, `-t`

The team with main responsibility for the model

### `--frequency`, `-f`

How often the model should be updated, as defined by name of frequency tag

### `--service-consumers`, `-sc`

Which service consumers that need access to the model

### `--access-policy`, `-ap`

What the access policy should be for the model
