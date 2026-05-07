from typing import List

from dbt_dry_run.models import TableField, BigQueryFieldType
from dbt_dry_run.models.table import FieldPath, Table
from dbt_dry_run.schema_change import (
    collect_field_paths_for_table,
    get_model_fields_not_present_in_target,
    add_new_fields_to_table,
    add_field_paths_to_struct,
    assert_no_nested_fields_removed_from_table,
)
import pytest
from dbt_dry_run.exception import SchemaChangeException


def test_collect_field_paths_for_table_should_collect_all_fields_and_their_paths() -> (
    None
):
    fields = [
        TableField(name="string_col", type=BigQueryFieldType.STRING),
        TableField(
            name="struct_col",
            type=BigQueryFieldType.STRUCT,
            fields=[
                TableField(
                    name="lv2",
                    type=BigQueryFieldType.STRING,
                    fields=[TableField(name="lv3", type=BigQueryFieldType.NUMERIC)],
                ),
            ],
        ),
    ]

    expected = [
        FieldPath(
            path=("string_col",),
            field=TableField(name="string_col", type=BigQueryFieldType.STRING),
        ),
        FieldPath(
            path=("struct_col",),
            field=TableField(
                name="struct_col",
                type=BigQueryFieldType.STRUCT,
                fields=[
                    TableField(
                        name="lv2",
                        type=BigQueryFieldType.STRING,
                        fields=[TableField(name="lv3", type=BigQueryFieldType.NUMERIC)],
                    )
                ],
            ),
        ),
        FieldPath(
            path=("struct_col", "lv2"),
            field=TableField(
                name="lv2",
                type=BigQueryFieldType.STRING,
                fields=[TableField(name="lv3", type=BigQueryFieldType.NUMERIC)],
            ),
        ),
        FieldPath(
            path=("struct_col", "lv2", "lv3"),
            field=TableField(name="lv3", type=BigQueryFieldType.NUMERIC),
        ),
    ]

    actual = collect_field_paths_for_table(fields)

    assert actual == expected


def test_get_model_fields_not_present_in_target_should_find_all_nested_fields_missing_from_target() -> (
    None
):
    dry_run_fields = [
        TableField(name="string_col", type=BigQueryFieldType.STRING),
        TableField(
            name="struct_col",
            type=BigQueryFieldType.STRUCT,
            fields=[
                TableField(
                    name="lv2",
                    type=BigQueryFieldType.STRING,
                    fields=[TableField(name="lv3", type=BigQueryFieldType.NUMERIC)],
                ),
                TableField(name="lv2_1", type=BigQueryFieldType.NUMERIC),
            ],
        ),
    ]

    target_fields = [
        TableField(name="string_col", type=BigQueryFieldType.STRING),
        TableField(
            name="struct_col",
            type=BigQueryFieldType.STRUCT,
            fields=[
                TableField(name="lv2", type=BigQueryFieldType.STRING),
            ],
        ),
    ]

    expected_target_fields = [
        FieldPath(
            path=("struct_col", "lv2", "lv3"),
            field=TableField(name="lv3", type=BigQueryFieldType.NUMERIC),
        ),
        FieldPath(
            path=("struct_col", "lv2_1"),
            field=TableField(name="lv2_1", type=BigQueryFieldType.NUMERIC),
        ),
    ]

    actual_missing_fields = get_model_fields_not_present_in_target(
        dry_run_fields, target_fields
    )

    assert actual_missing_fields == expected_target_fields


def test_assert_no_nested_fields_removed_from_table_should_raise_exception_if_field_is_removed_from_struct() -> (
    None
):
    dry_run_fields = [
        TableField(
            name="struct_col",
            type=BigQueryFieldType.STRUCT,
            fields=[
                TableField(name="lv2_1", type=BigQueryFieldType.STRING),
                TableField(name="lv2_2", type=BigQueryFieldType.NUMERIC),
            ],
        ),
    ]

    target_fields = [
        TableField(
            name="struct_col",
            type=BigQueryFieldType.STRUCT,
            fields=[
                TableField(name="lv2_1", type=BigQueryFieldType.STRING),
            ],
        ),
    ]

    with pytest.raises(SchemaChangeException) as exc_info:
        assert_no_nested_fields_removed_from_table(target_fields, dry_run_fields)

    assert (
        str(exc_info.value)
        == "Field 'struct_col.lv2_2' has been removed from a nested column"
    )


def test_assert_no_nested_fields_removed_from_table_should_not_raise_exception_if_field_is_removed_from_top_level() -> (
    None
):
    dry_run_fields = [
        TableField(name="string_col", type=BigQueryFieldType.STRING),
    ]

    target_fields = [
        TableField(name="string_col", type=BigQueryFieldType.STRING),
        TableField(name="removed_field", type=BigQueryFieldType.NUMERIC),
    ]

    expected_missing_fields: List[TableField] = []

    actual_missing_fields = get_model_fields_not_present_in_target(
        dry_run_fields, target_fields
    )
    assert_no_nested_fields_removed_from_table(dry_run_fields, target_fields)

    assert actual_missing_fields == expected_missing_fields


def test_add_field_paths_to_struct_should_add_missing_fields_to_correct_parent() -> (
    None
):
    target_field = TableField(
        name="struct_col",
        type=BigQueryFieldType.STRUCT,
        fields=[
            TableField(name="lv2", type=BigQueryFieldType.STRING),
        ],
    )

    missing_fields = [
        FieldPath(
            path=("struct_col", "lv2", "lv3"),
            field=TableField(name="lv3", type=BigQueryFieldType.NUMERIC),
        )
    ]

    expected_field = TableField(
        name="struct_col",
        type=BigQueryFieldType.STRUCT,
        fields=[
            TableField(
                name="lv2",
                type=BigQueryFieldType.STRING,
                fields=[TableField(name="lv3", type=BigQueryFieldType.NUMERIC)],
            ),
        ],
    )

    actual_field = add_field_paths_to_struct(target_field, missing_fields)

    assert actual_field == expected_field


def test_add_field_paths_to_struct_should_not_update_field_if_child_field_does_not_belong() -> (
    None
):
    target_field = TableField(
        name="struct_col",
        type=BigQueryFieldType.STRUCT,
        fields=[
            TableField(name="lv1", type=BigQueryFieldType.STRING),
        ],
    )

    missing_fields = [
        FieldPath(
            path=("struct_col", "lv2", "lv3"),
            field=TableField(name="lv3", type=BigQueryFieldType.NUMERIC),
        )
    ]

    expected_field = TableField(
        name="struct_col",
        type=BigQueryFieldType.STRUCT,
        fields=[
            TableField(name="lv1", type=BigQueryFieldType.STRING),
        ],
    )

    actual_field = add_field_paths_to_struct(target_field, missing_fields)

    assert actual_field == expected_field


def test_add_new_fields_to_table_should_correctly_reconstructs_table() -> None:
    target_table = Table(
        fields=[
            TableField(name="string_col", type=BigQueryFieldType.STRING),
            TableField(
                name="struct_col",
                type=BigQueryFieldType.STRUCT,
                fields=[
                    TableField(name="lv2", type=BigQueryFieldType.STRING),
                ],
            ),
        ]
    )

    missing_fields = [
        FieldPath(
            path=("struct_col", "lv2", "lv3"),
            field=TableField(name="lv3", type=BigQueryFieldType.NUMERIC),
        )
    ]

    expected_fields = [
        TableField(name="string_col", type=BigQueryFieldType.STRING),
        TableField(
            name="struct_col",
            type=BigQueryFieldType.STRUCT,
            fields=[
                TableField(
                    name="lv2",
                    type=BigQueryFieldType.STRING,
                    fields=[TableField(name="lv3", type=BigQueryFieldType.NUMERIC)],
                ),
            ],
        ),
    ]

    actual_fields = add_new_fields_to_table(target_table, missing_fields)

    assert actual_fields == expected_fields


def test_add_new_fields_to_table_should_include_selected_top_level_fields() -> None:
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
        FieldPath(
            path=("struct_col", "field_2"),
            field=TableField(name="field_2", type=BigQueryFieldType.NUMERIC),
        ),
        FieldPath(
            path=("new_col",),
            field=TableField(name="new_col", type=BigQueryFieldType.STRING),
        ),
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

    actual_fields = add_new_fields_to_table(target_table, missing_fields)

    assert actual_fields == expected_fields
