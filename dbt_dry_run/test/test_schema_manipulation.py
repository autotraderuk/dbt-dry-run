from dbt_dry_run.models import TableField, BigQueryFieldType
from dbt_dry_run.schema_manipulation import (
    merge_table_fields,
)


def test_merge_table_fields_should_include_new_top_level_fields() -> None:
    table_1_fields = [
        TableField(name="col_1", type=BigQueryFieldType.STRING),
    ]

    table_2_fields = [
        TableField(name="col_1", type=BigQueryFieldType.STRING),
        TableField(name="new_col", type=BigQueryFieldType.STRING),
    ]

    expected_fields = [
        TableField(name="col_1", type=BigQueryFieldType.STRING),
        TableField(name="new_col", type=BigQueryFieldType.STRING),
    ]

    actual_fields = merge_table_fields(
        table_2_fields=table_1_fields, table_1_fields=table_2_fields
    )

    assert actual_fields == expected_fields


def test_merge_table_fields_should_include_new_nested_fields() -> None:
    table_fields_1 = [
        TableField(name="col_1", type=BigQueryFieldType.STRING),
        TableField(
            name="struct_col",
            type=BigQueryFieldType.STRUCT,
            fields=[TableField(name="field_1", type=BigQueryFieldType.STRING)],
        ),
    ]

    table_fields_2 = [
        TableField(
            name="struct_col",
            type=BigQueryFieldType.STRUCT,
            fields=[
                TableField(
                    name="field_1",
                    type=BigQueryFieldType.STRING,
                ),
                TableField(
                    name="field_2",
                    type=BigQueryFieldType.NUMERIC,
                ),
            ],
        ),
        TableField(name="new_col", type=BigQueryFieldType.STRING),
    ]

    expected_fields = [
        TableField(name="col_1", type=BigQueryFieldType.STRING),
        TableField(
            name="struct_col",
            type=BigQueryFieldType.STRUCT,
            fields=[
                TableField(name="field_1", type=BigQueryFieldType.STRING),
                TableField(name="field_2", type=BigQueryFieldType.NUMERIC),
            ],
        ),
        TableField(name="new_col", type=BigQueryFieldType.STRING),
    ]

    actual_fields = merge_table_fields(
        table_2_fields=table_fields_1, table_1_fields=table_fields_2
    )

    assert actual_fields == expected_fields


def test_merge_table_fields_should_not_drop_top_level_fields_not_present_in_table_1_fields() -> (
    None
):
    table_fields_1 = [
        TableField(name="col_1", type=BigQueryFieldType.STRING),
        TableField(name="removed_col", type=BigQueryFieldType.STRING),
    ]

    table_fields_2 = [
        TableField(name="col_1", type=BigQueryFieldType.STRING),
    ]

    actual_fields = merge_table_fields(
        table_2_fields=table_fields_1, table_1_fields=table_fields_2
    )

    expected_fields = [
        TableField(name="col_1", type=BigQueryFieldType.STRING),
        TableField(name="removed_col", type=BigQueryFieldType.STRING),
    ]

    assert actual_fields == expected_fields


def test_merge_table_fields_should_not_drop_removed_nested_fields() -> None:
    table_fields_1 = [
        TableField(name="col_1", type=BigQueryFieldType.STRING),
        TableField(
            name="struct_col",
            type=BigQueryFieldType.STRUCT,
            fields=[
                TableField(name="kept_field", type=BigQueryFieldType.STRING),
                TableField(name="removed_field", type=BigQueryFieldType.NUMERIC),
            ],
        ),
    ]

    table_fields_2 = [
        TableField(name="col_1", type=BigQueryFieldType.STRING),
        TableField(
            name="struct_col",
            type=BigQueryFieldType.STRUCT,
            fields=[
                TableField(name="kept_field", type=BigQueryFieldType.STRING),
            ],
        ),
    ]

    expected_fields = [
        TableField(name="col_1", type=BigQueryFieldType.STRING),
        TableField(
            name="struct_col",
            type=BigQueryFieldType.STRUCT,
            fields=[
                TableField(name="kept_field", type=BigQueryFieldType.STRING),
                TableField(name="removed_field", type=BigQueryFieldType.NUMERIC),
            ],
        ),
    ]

    actual_fields = merge_table_fields(
        table_2_fields=table_fields_1, table_1_fields=table_fields_2
    )

    assert actual_fields == expected_fields
