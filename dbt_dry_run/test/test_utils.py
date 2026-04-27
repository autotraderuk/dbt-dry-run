from dbt_dry_run.models import TableField, BigQueryFieldType
from dbt_dry_run.models.table import FieldLineage, Table
from dbt_dry_run.utils import (
    collect_field_lineages,
    find_missing_fields,
    build_predicted_fields,
    add_missing_fields,
)


def test_collect_field_dicts_should_collect_all_fields_with_lineages() -> None:
    fields = [
        TableField(name="string_field", type=BigQueryFieldType.STRING),
        TableField(
            name="struct_field",
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
        FieldLineage(
            lineage="string_field",
            field=TableField(name="string_field", type=BigQueryFieldType.STRING),
        ),
        FieldLineage(
            lineage="struct_field",
            field=TableField(
                name="struct_field",
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
        FieldLineage(
            lineage="struct_field.lv2",
            field=TableField(
                name="lv2",
                type=BigQueryFieldType.STRING,
                fields=[TableField(name="lv3", type=BigQueryFieldType.NUMERIC)],
            ),
        ),
        FieldLineage(
            lineage="struct_field.lv2.lv3",
            field=TableField(name="lv3", type=BigQueryFieldType.NUMERIC),
        ),
    ]

    actual = collect_field_lineages(fields)

    assert actual == expected


def test_find_missing_fields_should_find_all_nested_fields_missing_from_target_fields() -> (
    None
):
    dry_run_fields = [
        TableField(name="string_field", type=BigQueryFieldType.STRING),
        TableField(
            name="struct_field",
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
        TableField(name="string_field", type=BigQueryFieldType.STRING),
        TableField(
            name="struct_field",
            type=BigQueryFieldType.STRUCT,
            fields=[
                TableField(name="lv2", type=BigQueryFieldType.STRING),
            ],
        ),
    ]

    expected_target_fields = [
        FieldLineage(
            lineage="struct_field.lv2.lv3",
            field=TableField(name="lv3", type=BigQueryFieldType.NUMERIC),
        ),
        FieldLineage(
            lineage="struct_field.lv2_1",
            field=TableField(name="lv2_1", type=BigQueryFieldType.NUMERIC),
        ),
    ]

    actual_missing_fields = find_missing_fields(dry_run_fields, target_fields)

    assert actual_missing_fields == expected_target_fields


def test_add_missing_fields_should_add_missing_fields_to_correct_parent() -> None:
    target_field = TableField(
        name="struct_field",
        type=BigQueryFieldType.STRUCT,
        fields=[
            TableField(name="lv2", type=BigQueryFieldType.STRING),
        ],
    )

    missing_fields = [
        FieldLineage(
            lineage="struct_field.lv2.lv3",
            field=TableField(name="lv3", type=BigQueryFieldType.NUMERIC),
        )
    ]

    expected_field = TableField(
        name="struct_field",
        type=BigQueryFieldType.STRUCT,
        fields=[
            TableField(
                name="lv2",
                type=BigQueryFieldType.STRING,
                fields=[TableField(name="lv3", type=BigQueryFieldType.NUMERIC)],
            ),
        ],
    )

    actual_field = add_missing_fields(target_field, missing_fields)

    assert actual_field == expected_field


def test_add_missing_fields_should_not_update_field_if_child_field_does_not_belong() -> (
    None
):
    target_field = TableField(
        name="struct_field",
        type=BigQueryFieldType.STRUCT,
        fields=[
            TableField(name="lv1", type=BigQueryFieldType.STRING),
        ],
    )

    missing_fields = [
        FieldLineage(
            lineage="struct_field.lv2.lv3",
            field=TableField(name="lv3", type=BigQueryFieldType.NUMERIC),
        )
    ]

    expected_field = TableField(
        name="struct_field",
        type=BigQueryFieldType.STRUCT,
        fields=[
            TableField(name="lv1", type=BigQueryFieldType.STRING),
        ],
    )

    actual_field = add_missing_fields(target_field, missing_fields)

    assert actual_field == expected_field


def test_build_predicted_table_correctly_reconstructs_table() -> None:
    target_table = Table(
        fields=[
            TableField(name="string_field", type=BigQueryFieldType.STRING),
            TableField(
                name="struct_field",
                type=BigQueryFieldType.STRUCT,
                fields=[
                    TableField(name="lv2", type=BigQueryFieldType.STRING),
                ],
            ),
        ]
    )

    missing_fields = [
        FieldLineage(
            lineage="struct_field.lv2.lv3",
            field=TableField(name="lv3", type=BigQueryFieldType.NUMERIC),
        )
    ]

    expected_predicted_fields = [
        TableField(name="string_field", type=BigQueryFieldType.STRING),
        TableField(
            name="struct_field",
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

    actual_predicted_fields = build_predicted_fields(target_table, missing_fields)

    assert actual_predicted_fields == expected_predicted_fields


def test_build_predicted_fields_should_filter_top_level_fields_and_preserve_nested_fields() -> (
    None
):
    target_table = Table(
        fields=[
            TableField(name="col_1", type=BigQueryFieldType.STRING),
            TableField(
                name="struct_field",
                type=BigQueryFieldType.STRUCT,
                fields=[TableField(name="nested_col_1", type=BigQueryFieldType.STRING)],
            ),
        ]
    )

    missing_fields = [
        FieldLineage(
            lineage="struct_field.nested_col_2",
            field=TableField(name="nested_col_2", type=BigQueryFieldType.NUMERIC),
        ),
        FieldLineage(
            lineage="new_col",
            field=TableField(name="new_col", type=BigQueryFieldType.STRING),
        ),
    ]

    expected_predicted_fields = [
        TableField(
            name="struct_field",
            type=BigQueryFieldType.STRUCT,
            fields=[
                TableField(name="nested_col_1", type=BigQueryFieldType.STRING),
                TableField(name="nested_col_2", type=BigQueryFieldType.NUMERIC),
            ],
        ),
        TableField(name="new_col", type=BigQueryFieldType.STRING),
    ]

    actual_predicted_fields = build_predicted_fields(
        target_table,
        missing_fields,
        included_top_level_field_names={"struct_field", "new_col"},
    )

    assert actual_predicted_fields == expected_predicted_fields
