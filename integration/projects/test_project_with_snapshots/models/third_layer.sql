{{ config(alias="third_layer") }}

SELECT *
FROM {{ ref("second_layer_snapshot") }}