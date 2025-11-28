from integration.conftest import CompletedDryRun


def test_failure(dry_run_result: CompletedDryRun) -> None:
    assert dry_run_result.get_report().success is False
