# `dbtwiz model lint`

Run sqlfmt --diff and sqlfluff lint for staged and/or defined sql files.

## Required arguments

- `model_names`: Models to lint.

## Options

### `--staged`, `-s`

Whether to lint staged sql files.

## Examples

Lint any number of given models, e.g.:
```
dbtwiz model lint mrt_siteconfig__site_groups mrt_siteconfig__sites
```

Lint models you have changed and staged:
```
dbtwiz model lint -s
```

It's also possible to combine the two:
```
dbtwiz model lint mrt_siteconfig__site_groups mrt_siteconfig__sites -s
```
