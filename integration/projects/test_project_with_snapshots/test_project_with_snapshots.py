from dbt_dry_run.models.manifest import SnapshotMetaColumnName
from integration.conftest import CompletedDryRun
from integration.utils import get_report_node_by_id, assert_report_node_has_columns

DBT_ALWAYS_ON_SNAPSHOT_COLUMN_NAMES = {
    SnapshotMetaColumnName.DBT_SCD_ID,
    SnapshotMetaColumnName.DBT_UPDATED_AT,
    SnapshotMetaColumnName.DBT_VALID_FROM,
    SnapshotMetaColumnName.DBT_VALID_TO,
}


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
        node, {"a", "b", "c", "updated_at", *DBT_ALWAYS_ON_SNAPSHOT_COLUMN_NAMES}
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
    assert_report_node_has_columns(
        node, {"a", "b", "c", *DBT_ALWAYS_ON_SNAPSHOT_COLUMN_NAMES}
    )


def test_case_single_column_succeeds(dry_run_result: CompletedDryRun):
    node = get_report_node_by_id(
        dry_run_result.report,
        "snapshot.test_project_with_snapshots.case_check_single_column_snapshot",
    )
    assert_report_node_has_columns(
        node, {"a", "b", "c", *DBT_ALWAYS_ON_SNAPSHOT_COLUMN_NAMES}
    )


def test_case_invalid_column_fails(dry_run_result: CompletedDryRun):
    node = get_report_node_by_id(
        dry_run_result.report,
        "snapshot.test_project_with_snapshots.case_check_invalid_column_snapshot",
    )
    assert not node.success
    assert node.error_message == "SnapshotConfigException"


def test_hard_deletes_creates_is_deleted_column(dry_run_result: CompletedDryRun):
    node = get_report_node_by_id(
        dry_run_result.report,
        "snapshot.test_project_with_snapshots.case_check_hard_deletes",
    )
    assert_report_node_has_columns(
        node,
        {
            "a",
            "b",
            "c",
            *DBT_ALWAYS_ON_SNAPSHOT_COLUMN_NAMES,
            SnapshotMetaColumnName.DBT_IS_DELETED,
        },
    )
