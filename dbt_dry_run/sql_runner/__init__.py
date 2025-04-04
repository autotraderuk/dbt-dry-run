from abc import ABCMeta, abstractmethod
from typing import Optional, Tuple

import agate

from dbt_dry_run.adapter.service import ProjectService
from dbt_dry_run.models import Table
from dbt_dry_run.models.manifest import Node
from dbt_dry_run.results import DryRunStatus


class SQLRunner(metaclass=ABCMeta):
    """
    Used to adapt to multiple warehouse backends
    """

    def __init__(self, project: ProjectService):
        self._project = project

    @abstractmethod
    def node_exists(self, node: Node) -> bool:
        ...

    @abstractmethod
    def get_node_schema(self, node: Node) -> Optional[Table]:
        ...

    @abstractmethod
    def query(
        self, sql: str
    ) -> Tuple[DryRunStatus, Optional[Table], Optional[Exception]]:
        ...

    def convert_agate_type(
        self, agate_table: agate.Table, col_idx: int
    ) -> Optional[str]:
        return self._project.adapter.convert_agate_type(agate_table, col_idx)
