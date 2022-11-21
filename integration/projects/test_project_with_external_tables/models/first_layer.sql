{{ config(alias="first_layer") }}

SELECT *
FROM {{ source("external", "src_external1") }}