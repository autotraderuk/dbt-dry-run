{{
    config(
        materialized="incremental",
        full_refresh=true
    )
}}

SELECT
   existing_column,
   new_column
FROM (SELECT "foo" as existing_column, "bar" as new_column)
