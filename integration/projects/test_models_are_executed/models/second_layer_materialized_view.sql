{{
config(
    alias="second_layer_materialized_view",
    materialized="materialized_view")
}}

SELECT *
FROM {{ ref("first_layer") }}