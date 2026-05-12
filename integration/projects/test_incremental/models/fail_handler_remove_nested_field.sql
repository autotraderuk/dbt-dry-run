{{
    config(
        materialized="incremental",
        on_schema_change="fail"
    )
}}

SELECT STRUCT("foo" AS kept_field) AS my_struct
