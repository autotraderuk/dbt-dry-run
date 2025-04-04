import re
from typing import List, Set, Tuple

from dbt_dry_run.models import Report, ReportNode
from dbt_dry_run.models.report import ReportLintingError
from dbt_dry_run.results import (
    DryRunResult,
    DryRunStatus,
    LintingError,
    LintingStatus,
    Results,
)

QUERY_JOB_SQL_FOLLOWS = "-----Query Job SQL Follows-----"
QUERY_JOB_HEADER = re.compile(r"|    .    ", re.MULTILINE)


def _map_column_errors(column_errors: List[LintingError]) -> List[ReportLintingError]:
    return list(
        map(
            lambda err: ReportLintingError(rule=err.rule, message=err.message),
            column_errors,
        )
    )


class ResultReporter:
    def __init__(self, results: Results, exclude: Set[str], verbose: bool = False):
        self._results = results
        self._exclude = exclude
        self._verbose = verbose

    def _report_failure_summary(
        self, failures: List[Tuple[DryRunResult, bool]]
    ) -> None:
        print(f"Total {len(failures)} failures:")
        for index, (failure, excluded) in enumerate(failures):
            if failure.exception:
                exception_col = failure.exception.__class__.__name__
            elif failure.linting_status == LintingStatus.FAILURE:
                exception_col = "LINTING"
            else:
                exception_col = "UNKNOWN"
            excluded_col = "EXCLUDED" if excluded else "ERROR"
            print(
                f"{index + 1}\t:\t{failure.node.unique_id}\t:\t{exception_col}\t:\t{excluded_col}"
            )

    def get_report(self) -> Report:
        report_nodes: List[ReportNode] = []
        success = True
        node_count = 0
        failure_count = 0
        failed_node_ids: List[str] = []

        for result in self._results.values():
            exception_type = (
                result.exception.__class__.__name__ if result.exception else None
            )
            new_node = ReportNode(
                unique_id=result.node.unique_id,
                success=result.status == DryRunStatus.SUCCESS,
                status=result.status,
                error_message=exception_type,
                table=result.table,
                linting_status=result.linting_status,
                linting_errors=_map_column_errors(result.linting_errors),
            )
            report_nodes.append(new_node)

            node_count += 1
            if not new_node.success or new_node.linting_status == LintingStatus.FAILURE:
                success = False
                failure_count += 1
                failed_node_ids.append(new_node.unique_id)

        report = Report(
            success=success,
            execution_time=self._results.execution_time_in_seconds,
            node_count=node_count,
            failure_count=failure_count,
            failed_node_ids=failed_node_ids,
            nodes=report_nodes,
        )

        return report

    def report_and_check_results(self) -> int:
        failures: List[Tuple[DryRunResult, bool]] = []
        for result in self._results.values():
            if result.status != DryRunStatus.SUCCESS and result.exception:
                print(f"Node {result.node.unique_id} failed with exception:")
                self._print_full_exception(result.exception)
                failures.append((result, result.node.unique_id in self._exclude))
            elif result.linting_status == LintingStatus.FAILURE:
                print(
                    f"Node {result.node.unique_id} failed linting with rule violations:"
                )
                for err in result.linting_errors:
                    print(f"\t{err.rule} : {err.message}")
                failures.append((result, False))

        if failures:
            print("")
            self._report_failure_summary(failures)
        included_failures = [f for f in failures if not f[1]]

        success = not len(included_failures)
        excluded_failures = len(included_failures) != len(failures)
        if success and excluded_failures:
            print("DRY RUN SUCCESS! (With excluded failures)")
        elif success:
            print("DRY RUN SUCCESS!")
        else:
            print("DRY RUN FAILURE!")

        return int(len(included_failures) > 0)

    def _print_full_exception(self, exception: Exception) -> None:
        error_message = str(exception)
        if not self._verbose:
            query_job_slq_position = error_message.find(QUERY_JOB_SQL_FOLLOWS)
            if query_job_slq_position >= 0:
                error_message = error_message[:query_job_slq_position].strip()
        else:
            error_message = QUERY_JOB_HEADER.sub(error_message, "")
        print(error_message)
