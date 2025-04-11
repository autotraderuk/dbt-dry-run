from dbt_dry_run.node_runner.snapshot_runner import DBT_SNAPSHOT_FIELDS
from integration.conftest import CompletedDryRun
from integration.utils import get_report_node_by_id, assert_report_node_has_columns

DBT_SNAPSHOT_COLUMN_NAMES = set([field.name for field in DBT_SNAPSHOT_FIELDS])


def test_case_invalid_unique_key_fails(dry_run_result: CompletedDryRun):
    node = get_report_node_by_id(
        dry_run_result.report,
        "snapshot.test_project_with_snapshots.case_invalid_unique_key_snapshot",
    )
    assert not node.success
    assert node.error_message == "SnapshotConfigException"


def test_case_timestamp_column_succeeds(dry_run_result: CompletedDryRun):
    node = get_report_node_by_id(
        dry_run_result.report,
        "snapshot.test_project_with_snapshots.case_timestamp_column_snapshot",
    )
    assert_report_node_has_columns(
        node, {"a", "b", "c", "updated_at", *DBT_SNAPSHOT_COLUMN_NAMES}
    )


def test_case_invalid_timestamp_column_fails(dry_run_result: CompletedDryRun):
    node = get_report_node_by_id(
        dry_run_result.report,
        "snapshot.test_project_with_snapshots.case_invalid_timestamp_column_snapshot",
    )
    assert not node.success
    assert node.error_message == "SnapshotConfigException"


def test_case_all_succeeds(dry_run_result: CompletedDryRun):
    node = get_report_node_by_id(
        dry_run_result.report,
        "snapshot.test_project_with_snapshots.case_check_all_snapshot",
    )
    assert_report_node_has_columns(node, {"a", "b", "c", *DBT_SNAPSHOT_COLUMN_NAMES})


def test_case_single_column_succeeds(dry_run_result: CompletedDryRun):
    node = get_report_node_by_id(
        dry_run_result.report,
        "snapshot.test_project_with_snapshots.case_check_single_column_snapshot",
    )
    assert_report_node_has_columns(node, {"a", "b", "c", *DBT_SNAPSHOT_COLUMN_NAMES})


def test_case_invalid_column_fails(dry_run_result: CompletedDryRun):
    node = get_report_node_by_id(
        dry_run_result.report,
        "snapshot.test_project_with_snapshots.case_check_invalid_column_snapshot",
    )
    assert not node.success
    assert node.error_message == "SnapshotConfigException"
