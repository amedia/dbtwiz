# `dbtwiz admin sync-descriptions`

Sync dbt descriptions from a manifest to BigQuery tables and columns.

Reads descriptions from the production dbt manifest and compares them with the current
BigQuery table and column descriptions. In dry-run mode (default), it lists which tables
have outdated descriptions without making any changes. Use `--apply` to push the updates.

Only columns documented in the manifest are updated; undocumented columns keep their
existing BigQuery descriptions unchanged. Ephemeral models and tables not found in
BigQuery are skipped.

## Options

### `--manifest-path`

Path to dbt manifest.json. Defaults to the prod manifest download location.

### `--model-names`, `-m`

Limit sync to specific model name(s). Can be repeated.

### `--dry-run`

Show changes without applying them (default: --dry-run)

### `--impersonate`

Impersonate the service account configured as service_account_identifier in pyproject.toml.
