from abc import ABCMeta, abstractmethod

from dbt_dry_run.models.manifest import Node
from dbt_dry_run.results import DryRunResult, Results
from dbt_dry_run.sql_runner import SQLRunner


class NodeRunner(metaclass=ABCMeta):
    """
    Runs a node and returns a result
    """

    resource_type: str

    def __init__(self, sql_runner: SQLRunner, results: Results):
        self._sql_runner = sql_runner
        self._results = results

    @abstractmethod
    def run(self, node: Node) -> DryRunResult:
        ...
