# `dbtwiz model from_sql`

Convert a sql file to a dbt model by replacing table references with source and ref.

## Required arguments

- `file_path`: File path for sql file


## Examples

If you have a sql file opened where you have pasted a query from BigQuery, e.g.
```
select * from `amedia-adp-marts.adplogger.adplogger_pageviews_view`
union all
select * from `amedia-adp-prod`.arcalis_raw.arcalis_events_raw
union all
select * from amedia-data-restricted.memento.`user_mapping`
union all
select * from amedia-analytics-eu.dfp.p_NetworkImpressions_56257416
```

You can then run `dbtwiz model from-sql test.sql`.

When run, the command will replace the table names with either `ref` or `source`, if that table is a current model or source:
```
select * from {{ ref("mrt_adplogger__adplogger_pageviews_view") }}
union all
select * from {{ source("arcalis_raw", "arcalis_events_raw") }}
union all
select * from {{ source("memento", "user_mapping") }}
union all
select * from {{ source("dfp", "NetworkImpressions") }}
```
