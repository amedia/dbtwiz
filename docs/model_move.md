# `dbtwiz model move`

Moves a model by copying to a new location with a new name,
and/or by updating the references to the model by other dbt models.

By default, it will create the new model in the new location as a copy of the original but with its name changed.
It will also leave the original model in place, but will change the materialization to a view (unless it's an ephemeral model),
and will change the sql to be "select * from <new model name>". But if setting the parameter `safe` to False, the original model will be deleted.

The function also supports updating the references in all other dbt models from the old name to the new model name.
To do this, make sure "update-references" is one of the options used for the parameter `actions`.

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
