{{
    config(
        materialized="incremental",
        on_schema_change="append_new_columns"
    )
}}

SELECT
   my_string
FROM (SELECT "foo" as my_string)
