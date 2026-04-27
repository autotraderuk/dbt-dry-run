from dbt_dry_run.models import BigQueryFieldType, Table, TableField
from dbt_dry_run.models.dry_run_result import DryRunResult, DryRunStatus
from dbt_dry_run.scheduler import ManifestScheduler
from dbt_dry_run.schema_change_handlers import (
    append_new_columns_handler,
    sync_all_columns_handler,
)
from dbt_dry_run.test.utils import SimpleNode

A_NODE = SimpleNode(
    unique_id="node1", depends_on=[], resource_type=ManifestScheduler.MODEL
).to_node()


def test_append_handler_includes_schema_change_to_nested_field() -> None:
    model_table = Table(
        fields=[
            TableField(name="col_1", type=BigQueryFieldType.STRING),
            TableField(
                name="struct",
                type=BigQueryFieldType.STRUCT,
                fields=[
                    TableField(name="nested_col_1", type=BigQueryFieldType.STRING),
                    TableField(name="nested_col_2", type=BigQueryFieldType.STRING),
                ],
            ),
        ]
    )
    dry_run_result = DryRunResult(
        node=A_NODE,
        status=DryRunStatus.SUCCESS,
        table=model_table,
        exception=None,
    )
    target_table = Table(
        fields=[
            TableField(name="col_1", type=BigQueryFieldType.STRING),
            TableField(
                name="struct",
                type=BigQueryFieldType.STRUCT,
                fields=[TableField(name="nested_col_1", type=BigQueryFieldType.STRING)],
            ),
        ]
    )
    actual_result = append_new_columns_handler(dry_run_result, target_table)

    assert actual_result.table == model_table


def test_append_handler_preserves_existing_column_order() -> None:
    model_table = Table(
        fields=[
            TableField(name="col_1", type=BigQueryFieldType.STRING),
            TableField(name="col_2", type=BigQueryFieldType.STRING),
            TableField(name="col_3", type=BigQueryFieldType.STRING),
        ]
    )
    dry_run_result = DryRunResult(
        node=A_NODE,
        status=DryRunStatus.SUCCESS,
        table=model_table,
        exception=None,
    )
    target_table = Table(
        fields=[
            TableField(name="col_2", type=BigQueryFieldType.STRING),
            TableField(name="col_1", type=BigQueryFieldType.STRING),
        ]
    )
    actual_result = append_new_columns_handler(dry_run_result, target_table)

    expected_table = Table(
        fields=[
            TableField(name="col_2", type=BigQueryFieldType.STRING),
            TableField(name="col_1", type=BigQueryFieldType.STRING),
            TableField(name="col_3", type=BigQueryFieldType.STRING),
        ]
    )

    assert actual_result.table == expected_table


def test_append_handler_preserves_existing_nested_column_order() -> None:
    model_table = Table(
        fields=[
            TableField(name="col_1", type=BigQueryFieldType.STRING),
            TableField(
                name="struct",
                type=BigQueryFieldType.STRUCT,
                fields=[
                    TableField(
                        name="nested_struct",
                        type=BigQueryFieldType.STRUCT,
                        fields=[
                            TableField(
                                name="nested_col_1", type=BigQueryFieldType.STRING
                            ),
                            TableField(
                                name="nested_col_2", type=BigQueryFieldType.STRING
                            ),
                        ],
                    ),
                ],
            ),
        ]
    )
    dry_run_result = DryRunResult(
        node=A_NODE,
        status=DryRunStatus.SUCCESS,
        table=model_table,
        exception=None,
    )
    target_table = Table(
        fields=[
            TableField(name="col_1", type=BigQueryFieldType.STRING),
            TableField(
                name="struct",
                type=BigQueryFieldType.STRUCT,
                fields=[
                    TableField(
                        name="nested_struct",
                        type=BigQueryFieldType.STRUCT,
                        fields=[
                            TableField(
                                name="nested_col_2", type=BigQueryFieldType.STRING
                            )
                        ],
                    ),
                ],
            ),
        ]
    )
    actual_result = append_new_columns_handler(dry_run_result, target_table)

    expected_table = Table(
        fields=[
            TableField(name="col_1", type=BigQueryFieldType.STRING),
            TableField(
                name="struct",
                type=BigQueryFieldType.STRUCT,
                fields=[
                    TableField(
                        name="nested_struct",
                        type=BigQueryFieldType.STRUCT,
                        fields=[
                            TableField(
                                name="nested_col_2", type=BigQueryFieldType.STRING
                            ),
                            TableField(
                                name="nested_col_1", type=BigQueryFieldType.STRING
                            ),
                        ],
                    ),
                ],
            ),
        ]
    )

    assert actual_result.table == expected_table


def test_sync_handler_preserves_existing_column_order() -> None:
    model_table = Table(
        fields=[
            TableField(name="col_3", type=BigQueryFieldType.STRING),
            TableField(name="col_2", type=BigQueryFieldType.STRING),
            TableField(name="col_4", type=BigQueryFieldType.STRING),
        ]
    )
    dry_run_result = DryRunResult(
        node=A_NODE,
        status=DryRunStatus.SUCCESS,
        table=model_table,
        exception=None,
    )
    target_table = Table(
        fields=[
            TableField(name="col_1", type=BigQueryFieldType.STRING),
            TableField(name="col_2", type=BigQueryFieldType.STRING),
            TableField(name="col_3", type=BigQueryFieldType.STRING),
        ]
    )
    actual_result = sync_all_columns_handler(dry_run_result, target_table)

    expected_table = Table(
        fields=[
            TableField(name="col_2", type=BigQueryFieldType.STRING),
            TableField(name="col_3", type=BigQueryFieldType.STRING),
            TableField(name="col_4", type=BigQueryFieldType.STRING),
        ]
    )

    assert actual_result.table == expected_table


def test_sync_handler_should_not_remove_nested_fields_from_existing_structs() -> None:
    model_table = Table(
        fields=[
            TableField(
                name="struct_col",
                type=BigQueryFieldType.STRUCT,
                fields=[TableField(name="nested_col_1", type=BigQueryFieldType.STRING)],
            )
        ]
    )
    dry_run_result = DryRunResult(
        node=A_NODE,
        status=DryRunStatus.SUCCESS,
        table=model_table,
        exception=None,
    )
    target_table = Table(
        fields=[
            TableField(
                name="struct_col",
                type=BigQueryFieldType.STRUCT,
                fields=[
                    TableField(name="nested_col_1", type=BigQueryFieldType.STRING),
                    TableField(name="nested_col_2", type=BigQueryFieldType.STRING),
                ],
            )
        ]
    )

    actual_result = sync_all_columns_handler(dry_run_result, target_table)

    assert actual_result.table == target_table
