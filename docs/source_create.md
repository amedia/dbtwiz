# `dbtwiz source create`

Create new dbt source

Creates a new dbt source definition interactively.
    
    Guides you through selecting project, dataset, and tables with autocomplete.
    Automatically generates source YAML files with proper descriptions.
    
    The command will:
    1. Help you select the appropriate project and dataset
    2. Provide autocomplete for available tables
    3. Generate proper source YAML structure
    4. Set appropriate descriptions and metadata
    5. Handle both new source definitions and adding to existing ones
    
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
