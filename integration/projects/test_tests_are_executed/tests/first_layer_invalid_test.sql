SELECT *
FROM {{ ref("first_layer") }}
WHERE wrong_column != "wrong"