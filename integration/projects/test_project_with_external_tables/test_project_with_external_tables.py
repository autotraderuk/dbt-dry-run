from integration.conftest import CompletedDryRun
from integration.utils import (
    get_report_node_by_id,
    assert_report_node_has_columns,
    assert_node_failed_with_error,
)


def test_model_selecting_from_external_table_has_correct_schema(
    dry_run_result: CompletedDryRun,
) -> None:
    node = get_report_node_by_id(
        dry_run_result.get_report(),
        "model.test_project_with_external_tables.first_layer",
    )
    assert node.success
    assert_report_node_has_columns(
        node,
        {
            "rowkey",
            "events",
            "events.column",
            "events.column.name",
            "events.column.cell",
            "events.column.cell.value",
        },
    )


def test_model_selecting_from_external_table_raises_error_if_no_columns(
    dry_run_result: CompletedDryRun,
) -> None:
    assert_node_failed_with_error(
        dry_run_result.get_report(),
        "source.test_project_with_external_tables.external.src_external_no_schema",
        "InvalidColumnSpecification",
    )
