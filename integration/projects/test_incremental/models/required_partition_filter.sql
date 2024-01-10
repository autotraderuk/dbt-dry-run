{{
    config(
        materialized="incremental",
        on_schema_change="append_new_columns",
        require_partition_filter = true,
        partition_by={'field': 'snapshot_date', 'data_type': 'date'}
    )
}}

SELECT
   col_1,
   col_2,
   snapshot_date
FROM (SELECT "foo" as col_1, "bar" as col_2, DATE("2023-01-04") as snapshot_date)


