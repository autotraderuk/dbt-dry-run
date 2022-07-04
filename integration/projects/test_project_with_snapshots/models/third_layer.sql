{{ config(alias="third_layer") }}

SELECT *
FROM {{ ref("case_check_all_snapshot") }}