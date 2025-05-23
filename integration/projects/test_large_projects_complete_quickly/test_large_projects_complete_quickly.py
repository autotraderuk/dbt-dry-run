import pytest

from integration.conftest import CompletedDryRun
from integration.utils import assert_report_success


@pytest.mark.skip(reason="Flaky test. Only include if worried about performance")
def test_success(dry_run_result: CompletedDryRun):
    assert_report_success(dry_run_result)

    assert dry_run_result.report.execution_time < 10
