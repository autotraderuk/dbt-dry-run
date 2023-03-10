from integration.conftest import DryRunResult


def test_failure(dry_run_result: DryRunResult):
    assert dry_run_result.report.success is False
