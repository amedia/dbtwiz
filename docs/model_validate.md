# `dbtwiz model validate`

Validates the yml and sql files for a model.

## Required arguments

- `model_path`: Path to model (sql or yml) to be validated.


## Examples

Example output when all is ok:
```
Validating yml exists: yml file ok
Validating yml definition: yml file name ok
Validating model config rules: model config rules ok
Validating yml columns: yml ok
Validating sql references: references ok
Validating sql with sqlfmt: validation ok
Validating sql with sqlfluff: validation ok
```

The model config rules step checks the model's config block against
`[[tool.dbtwiz.project.model_config_rules]]` in pyproject.toml, and reports
`no model config rules configured` for a project that declares none.

