{{
    config(
        materialized="incremental",
        on_schema_change="append_new_columns"
    )
}}

WITH RECURSIVE my_cte AS (
    SELECT "foo" AS join_key
)

SELECT
   my_struct
FROM (
    SELECT STRUCT("foo" AS my_string) AS my_struct, "foo" AS join_key
)
LEFT JOIN my_cte USING(join_key)
