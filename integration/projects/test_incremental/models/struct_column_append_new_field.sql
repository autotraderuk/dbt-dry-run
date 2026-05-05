{{
    config(
        materialized="incremental",
        on_schema_change="append_new_columns"
    )
}}

SELECT STRUCT("foo" AS my_string, "bar" AS my_string_2) AS my_struct