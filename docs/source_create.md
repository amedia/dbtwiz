# `dbtwiz source create`

Create new dbt source

When creating a new source, the function will ask a number of questions about the new source.

1. Project name: You can manually input the project, but it will autocomplete for the existing source projects.
2. Dataset name: Select one of the listed datasets that exist in the given project. If the dataset is new, it will ask for a description.
3. Table name(s): Select one or more of the listed tables that exist in the dataset and aren't defined as source yet. If you only selected one table, it will ask for a description.

## Options

### `--source-name`, `-s`

Where the source is located (existing alias used for project+dataset combination)

### `--source-description`, `-sd`

A short description for the project+dataset combination (if this combination is new)

### `--project-name`, `-p`

In which project the table is located

### `--dataset-name`, `-d`

In which dataset the table is located

### `--table-names`, `-t`

Name(s) of table(s) to be added as source(s)

### `--table-description`, `-td`

A short description for the table, if only one is provided
