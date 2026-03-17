# `dbtwiz admin update-clustering`

Update clustering configuration for a BigQuery table and re-cluster existing rows.

Updates the clustering configuration for a BigQuery table and re-clusters all existing rows.

The command will:
1. Update the clustering specification on the table metadata
2. Run an UPDATE statement to physically re-cluster all existing rows

If the table has `require_partition_filter` enabled, a partition filter is automatically added
to the UPDATE statement to satisfy BigQuery's requirement.

## Options

### `--table-id`, `-t`

Full table ID (project.dataset.table) of the table to update

### `--cluster-columns`, `-c`

Column to cluster on. Can be repeated to specify multiple columns in order.

## Examples

Update clustering on a table with a single column:
```shell
$ dbtwiz admin update-clustering --table-id my-project.my_dataset.my_table --cluster-column my_column
```

Update clustering with multiple columns (in order):
```shell
$ dbtwiz admin update-clustering --table-id my-project.my_dataset.my_table --cluster-column col_a --cluster-column col_b
```
