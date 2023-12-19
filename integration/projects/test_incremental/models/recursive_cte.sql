{{
    config(
        materialized="incremental",
        on_schema_change="ignore"
    )
}}

WITH RECURSIVE my_cte AS (
    SELECT "foo" as foo
)

SELECT
   my_string
FROM (SELECT "foo" as my_string, "bar" as foo)
LEFT JOIN my_cte using(foo)
