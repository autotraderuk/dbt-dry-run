{{ config(alias="model_with_sql_header_and_dbt_max_partition",
         materialized="incremental",
         incremental_strategy = 'insert_overwrite',
         partition_by = {'field': 'partition_date', 'data_type': 'date'})
}}

{% set incremental_filter = "DATE(_dbt_max_partition)" %}

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

SELECT *, yes_no_to_boolean("yes") as my_bool, DATE("2022-01-01") as partition_date
FROM (SELECT "a" as `a`, "b" as `b`, 1 as c)
WHERE DATE("2022-01-01") > {{ incremental_filter }}