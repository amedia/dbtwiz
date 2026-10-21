# `dbtwiz model validate`

Validates the yml and sql files for a model.

## Required arguments

- `model_path`: Path to model (sql or yml) to be validated.


## Examples

Example output when all is ok, on dbt-core:
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

On dbt v2 (dbt-core 1.x not installed), sqlfluff is replaced by `dbt lint`. With
`validate_with_dbt_check = true` in `[tool.dbtwiz.project]`, the project's own
`dbt check` checks also replace the rules dbtwiz checks itself: the model name's
layer and domain against its folders, a missing primary key and the model config
rules. The model is checked with `dbt check --select <model>`, and the yml
definition step keeps what concerns the yml itself (one model, named after its
file, with a primary key declared in one place). The setting is ignored on
dbt-core:
```
Validating yml exists: yml file ok
Validating yml definition: yml definition ok
Validating yml columns: yml ok
Validating sql references: references ok
Validating sql with sqlfmt: validation ok
Validating sql with dbt lint: validation ok
Validating with dbt check: checks ok
```

