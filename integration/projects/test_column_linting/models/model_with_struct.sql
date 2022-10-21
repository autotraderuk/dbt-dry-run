{{ config(alias="model_with_struct") }}

SELECT *
FROM (SELECT
             "a" as `a`,
             STRUCT("s1" as s1, 2 as s2, STRUCT("ss1" as ss1) as s3) as s
    )