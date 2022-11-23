SELECT *
FROM {{ ref("first_layer") }}
WHERE b != "b"