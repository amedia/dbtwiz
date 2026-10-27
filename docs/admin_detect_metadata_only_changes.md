# `dbtwiz admin detect-metadata-only-changes`

Detect whether every change against a state manifest is metadata only.

Finds out whether every change against a state manifest is metadata only, so a
CI build or deploy can skip running models, as it would for `[skip run]`.

Lists `state:modified` against the state, then again against a temporary copy of it in
which the metadata categories are taken from the current project. When the first
listing selects something and the second selects nothing, only metadata changed. dbt
itself decides what counts as modified both times; the copy is removed afterwards and
used for nothing else.

Categories:
- `description`: model, source and column descriptions
- `tags`: model, source and column tags
- `meta`: model, source and column meta
- `freshness`: source `freshness`, `loaded_at_field` and `loaded_at_query`

The categories come from `--ignore`, else `state_ignore` in pyproject.toml, else all of
them.

A skipped run still has to bring metadata to production: `meta` feeds grants, applied
by `dbtwiz admin update-grants`, and descriptions are applied by
`dbtwiz admin sync-descriptions`.

The command exits non-zero when dbt fails; treat that as a change beyond metadata.

## Options

### `--state`

Directory holding the manifest.json to compare against, as dbt's --state

### `--target`

dbt target to list with

### `--profiles-dir`

dbt profiles directory

### `--ignore`

Metadata category to treat as metadata. Can be repeated.

### `--result-path`

Write the result to this JSON file: metadata_only, categories, modified and modified_beyond_metadata

## Examples

Decide whether a deploy needs to run models:
```shell
$ dbtwiz admin detect-metadata-only-changes --state prod-state --target build --result-path result.json
$ jq .metadata_only result.json
```
