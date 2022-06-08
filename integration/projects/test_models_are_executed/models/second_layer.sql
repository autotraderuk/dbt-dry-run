{{ config(alias="second_layer") }}

SELECT *
FROM {{ ref("first_layer") }}
LEFT JOIN {{ ref("my_seed") }} USING(a)