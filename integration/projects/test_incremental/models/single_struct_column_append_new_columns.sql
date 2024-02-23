{{
    config(
        materialized="incremental",
        on_schema_change="append_new_columns"
    )
}}

SELECT
   my_struct
FROM (SELECT STRUCT("foo" as my_string, "bar" as my_string2) as my_struct)
