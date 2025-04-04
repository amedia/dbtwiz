# `dbtwiz model move`

Moves a model by copying to a new location with a new name,
and/or by updating the references to the model by other dbt models.

## Options

### `--old-model-name`, `-omn`

Current name for dbt model (excluding file type)

### `--new-model-name`, `-nmn`

New name for dbt model (excluding file type)

### `--old-folder-path`, `-ofp`

Current path for dbt model. Required if `move` is True.

### `--new-folder-path`, `-nfp`

New path for dbt model. Required if `move` is True.

### `--actions`, `-a`

Which move actions to execute

### `--safe`, `-s`

if moving model, whether to keep old model as a view to the new or do a hard move.
