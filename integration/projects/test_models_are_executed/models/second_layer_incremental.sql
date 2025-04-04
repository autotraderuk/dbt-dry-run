{{ config(alias="second_layer_incremental", materialized="incremental") }}

SELECT *
FROM {{ ref("first_layer") }}