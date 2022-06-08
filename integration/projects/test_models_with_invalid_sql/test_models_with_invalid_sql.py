from integration.conftest import IntegrationTestResult


def test_failure(dry_run_result: IntegrationTestResult):
    assert dry_run_result.report.success is False
