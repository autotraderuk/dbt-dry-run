{{
    config(
        materialized="incremental",
        partition_by={'field': 'snapshot_date', 'data_type': 'date'},
        sql_header="""
            CREATE TEMP FUNCTION myTempFunction(foo STRING)
            RETURNS STRING
            LANGUAGE js AS r\"\"\"
                return foo
            \"\"\";
        """
    )
}}

SELECT
    snapshot_date,
    my_string,
    myTempFunction(my_string) as my_func_output
FROM (SELECT DATE("2023-01-01") as snapshot_date, "foo" as my_string)
WHERE snapshot_date > _dbt_max_partition
