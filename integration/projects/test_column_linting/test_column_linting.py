from dbt_dry_run.models.report import LintingStatus
from integration.conftest import DryRunResult
from integration.utils import get_report_node_by_id


def test_linted_model_fails(dry_run_result: DryRunResult):
    node = get_report_node_by_id(
        dry_run_result.report, "model.test_column_linting.badly_documented_model"
    )
    expected_errors = {
        "Column not documented in metadata: 'c'",
        "Extra column in metadata: 'd'",
    }

    assert node.linting_status == LintingStatus.FAILURE
    assert len(node.linting_errors) == 2
    assert set(map(lambda err: err.message, node.linting_errors)) == expected_errors


def test_linting_disabled_model_skipped(dry_run_result: DryRunResult):
    node = get_report_node_by_id(
        dry_run_result.report, "model.test_column_linting.model_linting_disabled"
    )

    assert node.linting_status == LintingStatus.SKIPPED
    assert len(node.linting_errors) == 0


def test_linting_not_defined_model_skipped(dry_run_result: DryRunResult):
    node = get_report_node_by_id(
        dry_run_result.report, "model.test_column_linting.model_linting_not_specified"
    )

    assert node.linting_status == LintingStatus.SKIPPED
    assert len(node.linting_errors) == 0


def test_linting_model_with_structs_success(dry_run_result: DryRunResult):
    node = get_report_node_by_id(
        dry_run_result.report, "model.test_column_linting.model_with_struct"
    )
    assert node.linting_status == LintingStatus.SUCCESS


def test_linting_enabled_in_model_in_sub_dir(dry_run_result: DryRunResult):
    node = get_report_node_by_id(
        dry_run_result.report,
        "model.test_column_linting.badly_documented_model_in_sub_dir",
    )
    assert node.linting_status == LintingStatus.FAILURE
