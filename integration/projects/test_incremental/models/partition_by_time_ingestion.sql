{{
    config(
        materialized="incremental",
        partition_by={
            "field": "executed_at",
            "data_type": "date",
            "time_ingestion_partitioning": true
        }
    )
}}

SELECT
    executed_at,
    col_1,
    col_2
FROM (SELECT DATE('2024-06-06') as executed_at, "foo" as col_1, "bar" as col_2)
