{{ config(alias="disabled_model", enabled=false) }}

SELECT *
FROM {{ ref("first_layer") }}
LEFT JOIN {{ ref("my_seed") }} USING(a)