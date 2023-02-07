{{
    config(alias="run_mart_model")
}}

SELECT *
FROM {{ ref("run_staging_model") }}