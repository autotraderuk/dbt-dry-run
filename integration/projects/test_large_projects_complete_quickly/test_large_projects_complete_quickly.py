from integration.conftest import IntegrationTestResult
from integration.utils import assert_report_success


def test_success(dry_run_result: IntegrationTestResult):
    assert_report_success(dry_run_result)

    assert dry_run_result.report.execution_time < 10
