from dbt_dry_run.models import TableField, BigQueryFieldType
from dbt_dry_run.utils import collect_field_dicts, append_new_fields


def test_collect_field_dicts_should_collect_all_fields_with_paths() -> None:
    fields = [
        TableField(name="col_1", type=BigQueryFieldType.STRING),
        TableField(
            name="struct",
            type=BigQueryFieldType.STRUCT,
            fields=[
                TableField(name="nested_col_1", type=BigQueryFieldType.STRING),
                TableField(name="nested_col_2", type=BigQueryFieldType.NUMERIC),
            ],
        ),
    ]

    expected = [
        {"col_1": TableField(name="col_1", type=BigQueryFieldType.STRING)},
        {"struct": TableField(name="struct", type=BigQueryFieldType.STRUCT)},
        {
            "struct.nested_col_1": TableField(
                name="nested_col_1", type=BigQueryFieldType.STRING
            )
        },
        {
            "struct.nested_col_2": TableField(
                name="nested_col_2", type=BigQueryFieldType.NUMERIC
            )
        },
    ]

    actual = collect_field_dicts(fields)

    assert actual == expected


def test_append_new_fields_should_add_new_fields_to_target_fields() -> None:
    dry_run_fields = [
        TableField(name="col_1", type=BigQueryFieldType.STRING),
        TableField(
            name="struct",
            type=BigQueryFieldType.STRUCT,
            fields=[
                TableField(name="nested_col_1", type=BigQueryFieldType.STRING),
                TableField(name="nested_col_2", type=BigQueryFieldType.NUMERIC),
            ],
        ),
    ]

    target_fields = [
        TableField(name="col_1", type=BigQueryFieldType.STRING),
        TableField(
            name="struct",
            type=BigQueryFieldType.STRUCT,
            fields=[
                TableField(name="nested_col_1", type=BigQueryFieldType.STRING),
            ],
        ),
    ]

    expected_target_fields = [
        {"col_1": TableField(name="col_1", type=BigQueryFieldType.STRING)},
        {"struct": TableField(name="struct", type=BigQueryFieldType.STRUCT)},
        {
            "struct.nested_col_1": TableField(
                name="nested_col_1", type=BigQueryFieldType.STRING
            )
        },
        {
            "struct.nested_col_2": TableField(
                name="nested_col_2", type=BigQueryFieldType.NUMERIC
            )
        },
    ]

    actual_missing_fields = append_new_fields(dry_run_fields, target_fields)

    assert actual_missing_fields == expected_target_fields
