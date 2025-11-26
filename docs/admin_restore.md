# `dbtwiz admin restore`

Restore a deleted BigQuery table from a snapshot using time travel.

Restores a deleted BigQuery table from a snapshot using BigQuery time travel.

The command will:
1. Verify that the table is actually deleted
2. Parse the provided timestamp into the correct format
3. Use BigQuery's time travel feature to restore from the snapshot
4. Create the recovered table with the specified name (or default to table_name_recovered)

**Note:** BigQuery time travel is limited to 7 days. You can only restore tables that were deleted within the past 7 days.

**Timestamp formats supported:**
- Epoch milliseconds (e.g., 1705315800000)
- ISO 8601 format (e.g., 2024-01-15T10:30:00)
- Date format (e.g., 2024-01-15 10:30:00)

## Required arguments

- `table_id`: Full table ID (project.dataset.table) of the deleted table to restore
- `timestamp`: Snapshot timestamp as epoch milliseconds or date format (YYYY-MM-DD HH:MM:SS or YYYY-MM-DDTHH:MM:SS)

## Options

### `--recovered-table-id`, `-r`

Full table ID for the recovered table. Defaults to original table name with '_recovered' suffix

### `--verbose`, `-v`

Output more info about what is going on

## Examples

Basic restore example with default recovered table name:
```shell
$ dbtwiz admin restore my-project.my_dataset.my_table 2024-01-15T10:30:00
```

Restore with custom recovered table name:
```shell
$ dbtwiz admin restore my-project.my_dataset.my_table 1705315800000 --recovered-table my-project.my_dataset.my_table_backup
```

Restore with epoch milliseconds:
```shell
$ dbtwiz admin restore my-project.my_dataset.my_table 1705315800000
```
