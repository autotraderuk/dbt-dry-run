{{
    config(
        materialized="incremental",
        on_schema_change="ignore"
    )
}}

SELECT
   col_1,
   col_2
FROM (SELECT "foo" as col_1, "bar" as col_2)
