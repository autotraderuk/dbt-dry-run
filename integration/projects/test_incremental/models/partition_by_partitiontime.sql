{{
    config(
        materialized="incremental",
        partition_by={
            "field": "_PARTITIONTIME",
            "data_type": "timestamp",
            "time_ingestion_partitioning": true
        }
    )
}}

SELECT
    _PARTITIONTIME,
    col_1,
    col_2
FROM (SELECT TIMESTAMP('2024-06-06 00:00:00') AS _PARTITIONTIME, "foo" AS col_1, "bar" AS col_2)
