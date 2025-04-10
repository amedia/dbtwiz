# `dbtwiz model fix`

Run sqlfmt and sqlfix for staged and/or defined sql files.

## Required arguments

- `model_names`: Models to fix.

## Options

### `--staged`, `-s`

Whether to fix staged sql files.

## Examples

Fix any number of given models, e.g.:
```
dbtwiz model fix mrt_siteconfig__site_groups mrt_siteconfig__sites
```

Fix models you have changed and staged:
```
dbtwiz model fix -s
```

It's also possible to combine the two:
```
dbtwiz model fix mrt_siteconfig__site_groups mrt_siteconfig__sites -s
```
