import pytest

from integration.conftest import CompletedDryRun
from integration.utils import assert_report_success


@pytest.mark.skip(reason="Flaky test. Only include if worried about performance")
def test_success(dry_run_result: CompletedDryRun) -> None:
    assert_report_success(dry_run_result)

    execution_time = dry_run_result.get_report().execution_time
    assert execution_time is not None, "Execution time should be set"
    assert execution_time < 10, (
        f"Large project should complete quickly but took {execution_time} seconds"
    )
