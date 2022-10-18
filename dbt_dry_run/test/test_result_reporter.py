from typing import List

import pytest

from dbt_dry_run.models import Table
from dbt_dry_run.result_reporter import ResultReporter
from dbt_dry_run.results import (
    DryRunResult,
    DryRunStatus,
    LintingError,
    LintingStatus,
    Results,
)
from dbt_dry_run.test.utils import SimpleNode


@pytest.fixture()
def successful_result() -> DryRunResult:
    return DryRunResult(
        node=SimpleNode(unique_id="A", depends_on=[]).to_node(),
        table=Table(fields=[]),
        status=DryRunStatus.SUCCESS,
        exception=None,
        linting_status=LintingStatus.SKIPPED,
    )


@pytest.fixture()
def failed_result() -> DryRunResult:
    return DryRunResult(
        node=SimpleNode(unique_id="B", depends_on=[]).to_node(),
        table=Table(fields=[]),
        status=DryRunStatus.FAILURE,
        exception=Exception("Oh no!"),
        linting_status=LintingStatus.SKIPPED,
    )


@pytest.fixture()
def failed_linting_result() -> DryRunResult:
    return DryRunResult(
        node=SimpleNode(unique_id="B", depends_on=[]).to_node(),
        table=Table(fields=[]),
        status=DryRunStatus.SUCCESS,
        exception=None,
        linting_status=LintingStatus.FAILURE,
        linting_errors=[
            LintingError(rule="TEST_LINTING_RULE", message="Linting wrong")
        ],
    )


def build_results(dry_run_results: List[DryRunResult]) -> Results:
    results = Results()
    for result in dry_run_results:
        results.add_result(result.node.unique_id, result)
    return results


def test_successful_results(successful_result: DryRunResult) -> None:
    success_results = build_results([successful_result])
    reporter = ResultReporter(success_results, set())
    assert reporter.report_and_check_results() == 0


def test_failed_results(
    successful_result: DryRunResult, failed_result: DryRunResult
) -> None:
    failed_results = build_results([successful_result, failed_result])
    reporter = ResultReporter(failed_results, set())
    assert reporter.report_and_check_results() == 1


def test_failed_linting_results(
    successful_result: DryRunResult, failed_linting_result: DryRunResult
) -> None:
    failed_results = build_results([successful_result, failed_linting_result])
    reporter = ResultReporter(failed_results, set())
    assert reporter.report_and_check_results() == 1
