from abc import ABCMeta, abstractmethod
from typing import Optional

from dbt_dry_run import flags
from dbt_dry_run.exception import NotCompiledException
from dbt_dry_run.models.dry_run_result import DryRunResult
from dbt_dry_run.models.manifest import Node
from dbt_dry_run.models.report import DryRunStatus
from dbt_dry_run.results import Results
from dbt_dry_run.sql_runner import SQLRunner


class NodeRunner(metaclass=ABCMeta):
    """
    Runs a node and returns a result
    """

    def __init__(self, sql_runner: SQLRunner, results: Results):
        self._sql_runner = sql_runner
        self._results = results

    @abstractmethod
    def run(self, node: Node) -> DryRunResult:
        ...

    def check_node_compiled(self, node: Node) -> Optional[DryRunResult]:
        if not node.compiled:
            if not flags.SKIP_NOT_COMPILED:
                return DryRunResult(
                    node=node,
                    table=None,
                    status=DryRunStatus.FAILURE,
                    exception=NotCompiledException(
                        f"Node {node.unique_id} was not compiled"
                    ),
                )
            else:
                return DryRunResult(
                    node,
                    table=None,
                    status=DryRunStatus.SKIPPED,
                    exception=None,
                )
        else:
            return None
