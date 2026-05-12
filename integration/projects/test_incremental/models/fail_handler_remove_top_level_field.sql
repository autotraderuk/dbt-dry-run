{{
    config(
        materialized="incremental",
        on_schema_change="fail"
    )
}}

SELECT
    my_string
FROM (SELECT "foo" as my_string)
