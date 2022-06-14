from integration.conftest import IntegrationTestResult
from integration.utils import assert_report_success, get_report_node_by_id, assert_report_node_has_columns


def test_success(dry_run_result: IntegrationTestResult):
    assert assert_report_success(dry_run_result)


def test_snapshot_schema_predicted(dry_run_result: IntegrationTestResult):
    node_id = get_report_node_by_id(dry_run_result.report, "snapshot.test_project_with_snapshots.second_layer_snapshot")
    assert_report_node_has_columns(node_id, {"a", "b", "c", 'updated_at'})
