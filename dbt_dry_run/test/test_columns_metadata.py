from typing import Dict, List

import pytest

from dbt_dry_run.columns_metadata import (
    REPEATED_SUFFIX,
    expand_table_fields,
    map_columns_to_table,
)
from dbt_dry_run.exception import InvalidColumnSpecification, UnknownDataTypeException
from dbt_dry_run.literals import enable_test_example_values
from dbt_dry_run.models import BigQueryFieldMode, BigQueryFieldType, Table, TableField
from dbt_dry_run.models.manifest import ManifestColumn
from dbt_dry_run.test.utils import field_with_name

enable_test_example_values(True)


def get_column_map(columns: List[ManifestColumn]) -> Dict[str, ManifestColumn]:
    return {col.name: col for col in columns}


def assert_columns_result_in_table(
    columns: List[ManifestColumn], expected: Table
) -> None:
    actual = map_columns_to_table(get_column_map(columns))
    assert (
        actual == expected
    ), f"SQL Literal:\n {actual} does not equal expected:\n {expected}"


def field_type_as_repeated(field_type: BigQueryFieldType) -> str:
    return field_type + REPEATED_SUFFIX


def test_expand_table_fields_with_column_names_with_no_nesting() -> None:
    table = Table(fields=[field_with_name("a"), field_with_name("b")])

    expected = {"a", "b"}
    actual = expand_table_fields(table)
    assert actual == expected


def test_expand_table_fields_with_struct() -> None:
    table = Table(
        fields=[
            field_with_name("a"),
            field_with_name(
                "struct",
                fields=[field_with_name("struct_1"), field_with_name("struct_2")],
            ),
        ]
    )

    expected = {"a", "struct", "struct.struct_1", "struct.struct_2"}
    actual = expand_table_fields(table)
    assert actual == expected


def test_expand_table_fields_with_nested_struct() -> None:
    table = Table(
        fields=[
            field_with_name("a"),
            field_with_name(
                "struct",
                fields=[
                    field_with_name("struct_1", fields=[field_with_name("struct_1_1")])
                ],
            ),
        ]
    )

    expected = {"a", "struct", "struct.struct_1", "struct.struct_1.struct_1_1"}
    actual = expand_table_fields(table)
    assert actual == expected


def test_map_columns_to_table_handles_flat_schema() -> None:
    columns = [
        ManifestColumn(name="a", data_type=BigQueryFieldType.STRING),
        ManifestColumn(name="b", data_type=BigQueryFieldType.NUMERIC),
    ]
    table = Table(
        fields=[
            TableField(
                name="a", type=BigQueryFieldType.STRING, mode=BigQueryFieldMode.NULLABLE
            ),
            TableField(
                name="b",
                type=BigQueryFieldType.NUMERIC,
                mode=BigQueryFieldMode.NULLABLE,
            ),
        ]
    )
    assert_columns_result_in_table(columns, table)


def test_map_columns_to_table_handles_repeated_fields() -> None:
    columns = [
        ManifestColumn(name="a", data_type=BigQueryFieldType.STRING),
        ManifestColumn(
            name="b", data_type=field_type_as_repeated(BigQueryFieldType.NUMERIC)
        ),
    ]
    table = Table(
        fields=[
            TableField(
                name="a", type=BigQueryFieldType.STRING, mode=BigQueryFieldMode.NULLABLE
            ),
            TableField(
                name="b",
                type=BigQueryFieldType.NUMERIC,
                mode=BigQueryFieldMode.REPEATED,
            ),
        ]
    )
    assert_columns_result_in_table(columns, table)


def test_map_columns_to_table_handles_record() -> None:
    columns = [
        ManifestColumn(name="a", data_type=BigQueryFieldType.STRING),
        ManifestColumn(name="b", data_type=BigQueryFieldType.RECORD),
        ManifestColumn(name="b.c", data_type=BigQueryFieldType.STRING),
        ManifestColumn(name="b.d", data_type=BigQueryFieldType.NUMERIC),
    ]
    table = Table(
        fields=[
            TableField(
                name="a", type=BigQueryFieldType.STRING, mode=BigQueryFieldMode.NULLABLE
            ),
            TableField(
                name="b",
                type=BigQueryFieldType.RECORD,
                mode=BigQueryFieldMode.NULLABLE,
                fields=[
                    TableField(
                        name="c",
                        type=BigQueryFieldType.STRING,
                        mode=BigQueryFieldMode.NULLABLE,
                    ),
                    TableField(
                        name="d",
                        type=BigQueryFieldType.NUMERIC,
                        mode=BigQueryFieldMode.NULLABLE,
                    ),
                ],
            ),
        ]
    )
    assert_columns_result_in_table(columns, table)


def test_map_columns_to_table_raises_error_when_using_unknown_data_type() -> None:
    columns = [
        ManifestColumn(name="a", data_type=BigQueryFieldType.STRING + "_WRONG"),
        ManifestColumn(name="b", data_type=BigQueryFieldType.STRING),
    ]
    with pytest.raises(UnknownDataTypeException):
        map_columns_to_table(get_column_map(columns))


def test_map_columns_to_table_raises_error_when_struct_root_not_defined() -> None:
    columns = [
        ManifestColumn(name="a", data_type=BigQueryFieldType.STRING),
        ManifestColumn(name="b.c", data_type=BigQueryFieldType.STRING),
        ManifestColumn(name="b.d", data_type=BigQueryFieldType.NUMERIC),
    ]
    with pytest.raises(InvalidColumnSpecification):
        map_columns_to_table(get_column_map(columns))
