from typing import List, Optional

from dbt_dry_run.linting.column_linting import (
    expand_table_fields,
    get_extra_documented_columns,
    get_undocumented_columns,
)
from dbt_dry_run.models import BigQueryFieldMode, BigQueryFieldType, Table, TableField
from dbt_dry_run.models.manifest import ManifestColumn


def field_with_name(
    name: str,
    type_: BigQueryFieldType = BigQueryFieldType.STRING,
    mode: BigQueryFieldMode = BigQueryFieldMode.NULLABLE,
    fields: Optional[List[TableField]] = None,
) -> TableField:
    return TableField(name=name, type=type_, mode=mode, fields=fields)


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


def test_get_extra_documented_columns_passes_if_no_extra_columns() -> None:
    table = Table(fields=[field_with_name("a")])
    manifest_columns = {"a": ManifestColumn(name="a", description="a column")}

    expected: List[str] = []
    actual = get_extra_documented_columns(manifest_columns, table)

    assert actual == expected


def test_get_extra_documented_columns_fails_if_extra_columns() -> None:
    table = Table(fields=[field_with_name("a")])
    manifest_columns = {
        "a": ManifestColumn(name="a", description="a column"),
        "b": ManifestColumn(name="a", description="an extra column"),
    }

    expected = ["Extra column in metadata: 'b'"]
    actual = get_extra_documented_columns(manifest_columns, table)

    assert actual == expected


def test_get_undocumented_columns_passes_if_all_columns_present() -> None:
    table = Table(fields=[field_with_name("a")])
    manifest_columns = {"a": ManifestColumn(name="a", description="a column")}

    expected: List[str] = []
    actual = get_undocumented_columns(manifest_columns, table)

    assert actual == expected


def test_get_undocumented_columns_fails_if_undocumented_columns() -> None:
    table = Table(fields=[field_with_name("a"), field_with_name("b")])
    manifest_columns = {
        "a": ManifestColumn(name="a", description="a column"),
        "c": ManifestColumn(name="c", description="an extra column"),
    }

    expected = ["Column not documented in metadata: 'b'"]
    actual = get_undocumented_columns(manifest_columns, table)

    assert actual == expected
