WITH
generated_structure AS (
    SELECT STRUCT(
        "field-1" AS field_1,
        2 AS field_2,
        STRUCT(
            "field-3-sub-field-1" AS field_3_sub_field_1,
            "field-3-sub-field-2" AS field_3_sub_field_2
        ) as field_3
    ) AS my_struct
),
table_for_array_generation AS (
    SELECT 1 AS aggregation_column, "row-1" AS col_1, 1 AS col_2
    UNION ALL
    SELECT 1 AS aggregation_column, "row-2" AS col_1, 2 AS col_2
),
generated_array AS (
    SELECT aggregation_column, ARRAY_AGG(STRUCT(col_1, col_2)) AS my_array_of_records
    FROM table_for_array_generation
    GROUP BY aggregation_column
),
simple_column_types AS (
    SELECT
        'foo' AS my_string,
        b'foo' AS my_bytes,
        1 AS my_integer,
        1 AS my_int64,
        1.0 AS my_float,
        1.0 AS my_float64,
        true AS my_boolean,
        true AS my_bool,
        TIMESTAMP('2021-01-01') AS my_timestamp,
        DATE('2021-01-01') AS my_date,
        TIME(12,0,0) AS my_time,
        DATETIME(2021,1,1,12,0,0) AS my_datetime,
        MAKE_INTERVAL(1) AS my_interval,
        ST_GeogPoint(0.0, 0.0) AS my_geography,
        CAST(1 AS NUMERIC) AS my_numeric,
        CAST(2 AS BIGNUMERIC) AS my_bignumeric,
        PARSE_JSON('{"a": 1}') AS my_json,
        RANGE(DATE '2022-12-01', DATE '2022-12-31') as my_range
),
all_column_types AS (
    SELECT * FROM simple_column_types
    LEFT OUTER JOIN generated_structure ON true
    LEFT OUTER JOIN generated_array ON true
)

SELECT
    my_string,
    my_bytes,
    my_integer,
    my_int64,
    my_float,
    my_float64,
    my_boolean,
    my_bool,
    my_timestamp,
    my_date,
    my_time,
    my_datetime,
    my_interval,
    my_geography,
    my_numeric,
    my_bignumeric,
    my_json,
    my_struct,
    my_array_of_records,
    my_range
FROM all_column_types
