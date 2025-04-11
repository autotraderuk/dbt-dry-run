from integration.conftest import CompletedDryRun
from integration.utils import (
    assert_node_was_successful,
    assert_node_failed,
    assert_report_produced,
)


def test_valid_tests_are_run(dry_run_result: CompletedDryRun):
    report = assert_report_produced(dry_run_result)
    assert_node_was_successful(
        report, "test.test_tests_are_executed.first_layer_check_b_is_b"
    )


def test_invalid_tests_fail(dry_run_result: CompletedDryRun):
    report = assert_report_produced(dry_run_result)
    assert_node_failed(report, "test.test_tests_are_executed.first_layer_invalid_test")


def test_tests_for_missing_models_are_not_executed(dry_run_result: CompletedDryRun):
    report = assert_report_produced(dry_run_result)
    assert (
        "test.test_tests_are_executed.not_null_missing_model_some_column.229c1b7374"
        not in set(n.unique_id for n in report.nodes)
    )
