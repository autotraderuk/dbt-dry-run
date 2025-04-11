from integration.conftest import CompletedDryRun


def test_failure(dry_run_result: CompletedDryRun):
    assert dry_run_result.report.success is False
