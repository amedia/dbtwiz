# `dbtwiz admin update-grants`

Update BigQuery table IAM grants for all dbt models based on manifest configuration.

Reads the dbt manifest and dbt_project.yml vars to resolve the desired IAM grants
for every model, then fetches current table-level IAM policies in parallel and applies
the minimal set of changes needed to reach the desired state.

Grant configuration is resolved from the following sources (in order):
- Explicit `grants` config on the model
- `meta.teams` resolved via the `teams` var in dbt_project.yml
- `meta.access-policy` resolved via the `access-policies` var in dbt_project.yml
- `meta.service-consumers` resolved via the `service-consumers` var in dbt_project.yml
- Auto-grant to `grants_open_access_group` for models with `access: protected` or `access: public`

Models are skipped when they have `meta.skip_grants: true`, use Iceberg (`catalog_name`),
are ephemeral, or belong to a schema listed in `grants_skip_schemas` (pyproject.toml).

## Options

### `--manifest-path`

Path to dbt manifest.json. Defaults to the prod manifest download location.

### `--dry-run`

Show changes without executing them (default)

### `--resolve-only`

Only resolve desired grants from the manifest without querying BigQuery

### `--impersonate`

Impersonate the service account configured as service_account_identifier in pyproject.toml.
