{{
    config(alias="skip_mart_model")
}}

SELECT *
FROM {{ ref("skip_staging_model") }}