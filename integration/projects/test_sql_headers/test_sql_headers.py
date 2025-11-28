from integration.conftest import CompletedDryRun
from integration.utils import get_report_node_by_id, assert_report_node_has_columns


def test_udf_column_present_and_model_runs(dry_run_result: CompletedDryRun) -> None:
    node = get_report_node_by_id(
        dry_run_result.get_report(),
        "model.test_sql_headers.sql_header_udf_and_dbt_max_partition",
    )

    assert node.success
    assert_report_node_has_columns(node, {"a", "b", "c", "my_bool", "partition_date"})


def test_udf_column_present(dry_run_result: CompletedDryRun) -> None:
    node = get_report_node_by_id(
        dry_run_result.get_report(), "model.test_sql_headers.sql_header_udf"
    )

    assert node.success
    assert_report_node_has_columns(node, {"a", "b", "c", "my_bool"})
