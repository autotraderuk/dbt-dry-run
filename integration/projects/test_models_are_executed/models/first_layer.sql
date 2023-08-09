{{ config(alias="first_layer") }}

-- TODO: This needs to be Snowflake comaptible
SELECT *
FROM (SELECT 'a' as `a`, 'b' as `b`, 1 as c)