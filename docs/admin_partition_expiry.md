# `dbtwiz admin partition-expiry`

Checks for mismatched partition expiry and allows updating to correct.

When run, the current partition expiration definition in BigQuery will be compared with the definition in dbt for the model:
```
partition_expiration_days: <number of days>
```
The tables with differing values will be listed, and it's then possible to select which tables to update partition expiration for.

When comparing, the function uses the production manifest rather then the local version.

## Options

### `--model-names`, `-m`

Name of model to be checked for partition expiry
