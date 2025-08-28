# `dbtwiz model create`

Create new dbt model.

Creates a new dbt model with proper folder structure and YAML configuration.

    Assumes dbt project structure: models/layer/domain/model_name.sql
    - Layers: staging (stg), intermediate (int), marts (mrt), bespoke (bsp)
    - Models are prefixed: stg_domain__name, int_domain__name, etc.

    Requires dbt_project.yml with teams, access-policies, and service-consumers variables.

    The command will guide you through:
    1. Selecting the appropriate layer and domain
    2. Naming the model with proper conventions
    3. Setting access policies and team ownership
    4. Configuring materialization and expiration settings
    5. Defining service consumer access requirements
    
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
