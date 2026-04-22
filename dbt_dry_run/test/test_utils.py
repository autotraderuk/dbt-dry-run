from dbt_dry_run.models import TableField, BigQueryFieldType
from dbt_dry_run.models.table import FieldLineage, Table
from dbt_dry_run.utils import collect_field_lineages, find_missing_fields, build_predicted_table


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
                        fields=[
                            TableField(name="lv3", type=BigQueryFieldType.NUMERIC)
                        ],
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
        )
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

    missing_fields = [FieldLineage(
            lineage="struct_field.lv2.lv3",
            field=TableField(name="lv3", type=BigQueryFieldType.NUMERIC),
        )
    ]

    expected_predicted_table = Table(
        fields=[
            TableField(name="string_field", type=BigQueryFieldType.STRING),
            TableField(
                name="struct_field",
                type=BigQueryFieldType.STRUCT,
                fields=[
                    TableField(name="lv2", type=BigQueryFieldType.STRING, fields=[TableField(name="lv3", type=BigQueryFieldType.NUMERIC)]),
                ]
            )
        ]
    )

    field_name = expected_predicted_table.fields[1].child_field_names
    actual_predicted_table = build_predicted_table(target_table, missing_fields)

    assert actual_predicted_table == expected_predicted_table
