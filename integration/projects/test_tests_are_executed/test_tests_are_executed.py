from integration.conftest import IntegrationTestResult
from integration.utils import assert_node_was_successful, assert_node_failed, \
    assert_report_produced


def test_valid_tests_are_run(dry_run_result: IntegrationTestResult):
    report = assert_report_produced(dry_run_result)
    assert_node_was_successful(report, "test.test_tests_are_executed.first_layer_check_b_is_b")


def test_invalid_tests_fail(dry_run_result: IntegrationTestResult):
    report = assert_report_produced(dry_run_result)
    assert_node_failed(report, "test.test_tests_are_executed.first_layer_invalid_test")
