{{ config(alias="model_with_sql_header",
         materialized="table")
}}

{% call set_sql_header(config) %}
CREATE TEMPORARY FUNCTION yes_no_to_boolean(answer STRING)
    RETURNS BOOLEAN AS (
    CASE
        WHEN LOWER(answer) = 'yes' THEN True
        WHEN LOWER(answer) = 'no' THEN False
        ELSE NULL
        END
    );
{%- endcall %}

SELECT *, yes_no_to_boolean("yes") as my_bool
FROM (SELECT "a" as `a`, "b" as `b`, 1 as c)