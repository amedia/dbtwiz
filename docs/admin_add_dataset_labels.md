# `dbtwiz admin add-dataset-labels`

Add the configured dataset labels to the datasets of the models in a manifest.

Adds the labels configured as dataset_labels in pyproject.toml to the datasets of
the models, seeds and snapshots in a dbt manifest. Run it after a deploy, the only time
dbt creates new datasets, so storage billing can be attributed to dbt.

Only datasets in the projects listed as dataset_labels_projects are labelled; a model
can write to a dataset in another team's project, which is left alone.

A label the dataset already has is left alone, also when its value differs (a warning is
shown), and labels not in dataset_labels are kept. In dry-run mode (default), it lists
which datasets are missing labels without making any changes. Use `--apply` to add them.
It stops at the first permission error, and exits with an error when any update failed.

## Options

### `--manifest-path`

Path to dbt manifest.json. Defaults to the prod manifest download location.

### `--dry-run`

Show changes without applying them (default: --dry-run)

### `--impersonate`

Impersonate the service account configured as service_account_identifier in pyproject.toml.
