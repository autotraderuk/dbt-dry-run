from dbt_dry_run.models import TableField, BigQueryFieldType
from dbt_dry_run.models.table import FieldLineage
from dbt_dry_run.utils import collect_field_lineages, find_missing_fields


def test_collect_field_dicts_should_collect_all_fields_with_lineages() -> None:
    fields = [
        TableField(name="string_field", type=BigQueryFieldType.STRING),
        TableField(
            name="nested_field",
            type=BigQueryFieldType.STRUCT,
            fields=[
                TableField(
                    name="lv_2",
                    type=BigQueryFieldType.STRING,
                    fields=[TableField(name="lv_3", type=BigQueryFieldType.NUMERIC)],
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
            lineage="nested_field",
            field=TableField(
                name="nested_field",
                type=BigQueryFieldType.STRUCT,
                fields=[
                    TableField(
                        name="lv_2",
                        type=BigQueryFieldType.STRING,
                        fields=[
                            TableField(name="lv_3", type=BigQueryFieldType.NUMERIC)
                        ],
                    )
                ],
            ),
        ),
        FieldLineage(
            lineage="nested_field.lv_2",
            field=TableField(
                name="lv_2",
                type=BigQueryFieldType.STRING,
                fields=[TableField(name="lv_3", type=BigQueryFieldType.NUMERIC)],
            ),
        ),
        FieldLineage(
            lineage="nested_field.lv_2.lv_3",
            field=TableField(name="lv_3", type=BigQueryFieldType.NUMERIC),
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
            name="nested_field",
            type=BigQueryFieldType.STRUCT,
            fields=[
                TableField(
                    name="lv_2",
                    type=BigQueryFieldType.STRING,
                    fields=[TableField(name="lv_3", type=BigQueryFieldType.NUMERIC)],
                ),
                TableField(name="lv_2_1", type=BigQueryFieldType.NUMERIC),
            ],
        ),
    ]

    target_fields = [
        TableField(name="string_field", type=BigQueryFieldType.STRING),
        TableField(
            name="nested_field",
            type=BigQueryFieldType.STRUCT,
            fields=[
                TableField(name="lv_2", type=BigQueryFieldType.STRING),
            ],
        ),
    ]

    expected_target_fields = [
        FieldLineage(
            lineage="nested_field.lv_2.lv_3",
            field=TableField(name="lv_3", type=BigQueryFieldType.NUMERIC),
        ),
        FieldLineage(
            lineage="nested_field.lv_2_1",
            field=TableField(name="lv_2_1", type=BigQueryFieldType.NUMERIC),
        ),
    ]

    actual_missing_fields = find_missing_fields(dry_run_fields, target_fields)

    assert actual_missing_fields == expected_target_fields


#
# def test_rebuild_nested_fields_correctly_reconstructs_table() -> None:
#     new_target_map = [
#         {"col_1": TableField(name="col_1", type=BigQueryFieldType.STRING)},
#         {"struct": TableField(name="struct", type=BigQueryFieldType.STRUCT)},
#         {
#             "struct.nested_col_1": TableField(
#                 name="nested_col_1", type=BigQueryFieldType.STRING
#             )
#         },
#         {
#             "struct.nested_col_2": TableField(
#                 name="nested_col_2", type=BigQueryFieldType.NUMERIC
#             )
#         },
#     ]
#
#     expected_target_table = Table(
#         fields=[
#             TableField(name="col_1", type=BigQueryFieldType.STRING),
#             TableField(
#                 name="struct",
#                 type=BigQueryFieldType.STRUCT,
#                 fields=[
#                     TableField(name="nested_col_1", type=BigQueryFieldType.STRING),
#                     TableField(name="nested_col_2", type=BigQueryFieldType.NUMERIC),
#                 ],
#             ),
#         ]
#     )
#
#     actual_target_table = rebuild_nested_fields(new_target_map)
#
#     assert actual_target_table == expected_target_table
