{{
    config(
        materialized="incremental"
    )
}}

SELECT
   existing_column,
   new_column
FROM (SELECT "foo" as existing_column, "bar" as new_column)
