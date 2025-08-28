# `dbtwiz model move`

Moves a model by copying to a new location with a new name,
and/or by updating the references to the model by other dbt models.

Moves or renames a dbt model with optional reference updates.

    By default, creates a copy at the new location and converts the original to a view
    pointing to the new model. Use --safe=false to delete the original instead.

    Use --action update-references to update all model references automatically.

    This command handles:
    1. Moving the model file to a new location
    2. Updating the model name in the file content
    3. Converting the old model to a view that references the new one
    4. Optionally updating all references in other models
    5. Maintaining data lineage and dependencies
    
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
