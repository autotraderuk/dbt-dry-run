{{ config(alias="model_with_struct") }}

SELECT *
FROM (SELECT "a" as `a`, STRUCT("s1" as s1, 2 as s2) as s)