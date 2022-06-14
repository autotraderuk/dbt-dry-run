import itertools
from abc import ABCMeta, abstractmethod
from typing import Dict, List, Tuple, Type

from dbt_dry_run.models.manifest import Node
from dbt_dry_run.results import DryRunResult, Results
from dbt_dry_run.sql_runner import SQLRunner


class NodeRunner(metaclass=ABCMeta):
    """
    Runs a node and returns a result
    """

    resource_type: Tuple[str, ...]

    def __init__(self, sql_runner: SQLRunner, results: Results):
        self._sql_runner = sql_runner
        self._results = results

    @abstractmethod
    def run(self, node: Node) -> DryRunResult:
        ...


def _get_runner_resource_types(
    runner: Type[NodeRunner],
) -> List[Tuple[str, Type[NodeRunner]]]:
    return [(resource_type, runner) for resource_type in runner.resource_type]


def get_runner_map(runners: List[Type[NodeRunner]]) -> Dict[str, Type[NodeRunner]]:
    return dict(
        itertools.chain(*[_get_runner_resource_types(runner) for runner in runners])
    )
