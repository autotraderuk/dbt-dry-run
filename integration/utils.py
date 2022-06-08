from typing import Set

from dbt_dry_run.models import Report, ReportNode
from integration.conftest import IntegrationTestResult


def assert_report_produced(result: IntegrationTestResult) -> Report:
    assert result.report
    return result.report


def assert_report_success(result: IntegrationTestResult) -> Report:
    assert result.report
    assert result.report.success
    return result.report


def assert_report_failure(result: IntegrationTestResult) -> Report:
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
    assert node.success


def assert_report_node_has_columns(node: ReportNode, columns: Set[str]) -> None:
    assert set(map(lambda field: field.name, node.table.fields)) == columns
