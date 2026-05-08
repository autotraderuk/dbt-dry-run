import pytest

from dbt_dry_run.exception import SchemaChangeException
from dbt_dry_run.models import TableField, BigQueryFieldType
from dbt_dry_run.models.table import Table
from dbt_dry_run.schema_manipulation import (
    merge_table_fields,
)


def test_update_table_schema_should_include_new_top_level_fields() -> None:
    target_table = Table(
        fields=[
            TableField(name="col_1", type=BigQueryFieldType.STRING),
        ]
    )

    missing_fields = [
        TableField(name="col_1", type=BigQueryFieldType.STRING),
        TableField(name="new_col", type=BigQueryFieldType.STRING),
    ]

    expected_fields = [
        TableField(name="col_1", type=BigQueryFieldType.STRING),
        TableField(name="new_col", type=BigQueryFieldType.STRING),
    ]

    actual_fields = merge_table_fields(
        new_table_fields=missing_fields, table=target_table
    )

    assert actual_fields == expected_fields


def test_update_table_schema_should_include_new_nested_fields() -> None:
    target_table = Table(
        fields=[
            TableField(name="col_1", type=BigQueryFieldType.STRING),
            TableField(
                name="struct_col",
                type=BigQueryFieldType.STRUCT,
                fields=[TableField(name="field_1", type=BigQueryFieldType.STRING)],
            ),
        ]
    )

    missing_fields = [
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
        table_fields_1=missing_fields, table_fields_2=target_table
    )

    assert actual_fields == expected_fields


def test_update_table_schema_should_not_drop_top_level_fields_not_present_in_new_table_fields() -> (
    None
):
    table = Table(
        fields=[
            TableField(name="col_1", type=BigQueryFieldType.STRING),
            TableField(name="removed_col", type=BigQueryFieldType.STRING),
        ]
    )

    new_table_fields = [
        TableField(name="col_1", type=BigQueryFieldType.STRING),
    ]

    actual_fields = merge_table_fields(table_fields_1=new_table_fields, table_fields_2=table)

    expected_fields = [
        TableField(name="col_1", type=BigQueryFieldType.STRING),
        TableField(name="removed_col", type=BigQueryFieldType.STRING),
    ]

    assert actual_fields == expected_fields


def test_update_table_schema_should_not_drop_removed_nested_fields_and_should_raise_schema_change_exception() -> (
    None
):
    table = Table(
        fields=[
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
    )

    new_table_fields = [
        TableField(name="col_1", type=BigQueryFieldType.STRING),
        TableField(
            name="struct_col",
            type=BigQueryFieldType.STRUCT,
            fields=[
                TableField(name="kept_field", type=BigQueryFieldType.STRING),
            ],
        ),
    ]

    with pytest.raises(SchemaChangeException) as exc_info:
        merge_table_fields(table_fields_1=new_table_fields, table_fields_2=table)

    assert (
        str(exc_info.value)
        == "Field 'struct_col.removed_field' has been removed from a nested column"
    )
