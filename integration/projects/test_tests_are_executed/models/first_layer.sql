{{ config(alias="first_layer") }}

SELECT *
FROM (SELECT "a" as `a`, "b" as `b`, 1 as c)