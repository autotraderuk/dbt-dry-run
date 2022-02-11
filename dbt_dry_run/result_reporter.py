from typing import List, Set, Tuple

from dbt_dry_run.models import DryRunResult, DryRunStatus
from dbt_dry_run.results import Results


class ResultReporter:
    def __init__(self, results: Results, exclude: Set[str]):
        self._results = results
        self._exclude = exclude

    def _report_failure_summary(
        self, failures: List[Tuple[DryRunResult, bool]]
    ) -> None:
        print(f"Total {len(failures)} failures:")
        for index, (failure, excluded) in enumerate(failures):
            if failure.exception:
                exception_col = failure.exception.__class__.__name__
            else:
                exception_col = "UNKNOWN"
            excluded_col = "EXCLUDED" if excluded else "ERROR"
            print(
                f"{index + 1}\t:\t{failure.node.unique_id}\t:\t{exception_col}\t:\t{excluded_col}"
            )

    def report_and_check_results(self) -> int:
        failures: List[Tuple[DryRunResult, bool]] = []
        for result in self._results.values():
            if result.status != DryRunStatus.SUCCESS:
                print(f"Node {result.node.unique_id} failed!")
                failures.append((result, result.node.unique_id in self._exclude))
                print(str(result.exception))
        print("")
        if failures:
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
