from typing import Set

from dbt_dry_run.models import Report, ReportNode
from dbt_dry_run.columns_metadata import expand_table_fields
from integration.conftest import DryRunResult


def assert_report_produced(result: DryRunResult) -> Report:
    assert result.report, f"Report is missing: {result.process.stdout.decode('utf-8')}\n{result.process.stderr.decode('utf-8')}"
    return result.report


def assert_report_success(result: DryRunResult) -> Report:
    assert result.report, f"Report is missing: {result.process.stdout.decode('utf-8')}\n{result.process.stderr.decode('utf-8')}"
    assert result.report.success, "Expected success but got failure"
    return result.report


def assert_report_failure(result: DryRunResult) -> Report:
    assert result.report
    assert not result.report.success
    return result.report


def get_report_node_by_id(report: Report, unique_id: str) -> ReportNode:
    for node in report.nodes:
        if node.unique_id == unique_id:
            return node
    raise KeyError(f"Could not find report node '{unique_id}'")


def assert_node_was_successful(report: Report, unique_id: str) -> None:
    node = get_report_node_by_id(report, unique_id)
    assert (
        node.success
    ), f"Expected node f{node.unique_id} to be successful but it failed with error: {node.error_message}"


def assert_node_failed_with_error(report: Report, unique_id: str, error: str) -> None:
    node = get_report_node_by_id(report, unique_id)
    assert (
        not node.success
    ), f"Expected node {node.unique_id} to fail but it was successful"
    assert (
        node.error_message == error
    ), f"Node failed but error message '{node.error_message}' did not match expected: '{error}'"


def assert_node_failed(report: Report, unique_id: str) -> None:
    node = get_report_node_by_id(report, unique_id)
    assert not node.success


def assert_report_node_has_columns(node: ReportNode, columns: Set[str]) -> None:
    column_names = set(expand_table_fields(node.table))
    assert (
        column_names == columns
    ), f"Report node {node.unique_id} columns: {column_names} does not have expected columns: {columns}"
