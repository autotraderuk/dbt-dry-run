{{
    config(
        materialized="incremental",
        on_schema_change="sync_all_columns"
    )
}}

SELECT STRUCT("foo" AS my_string_1) AS my_struct