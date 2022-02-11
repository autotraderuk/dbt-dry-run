from abc import ABCMeta, abstractmethod
from typing import Optional, Tuple

from dbt_dry_run.manifest import Node
from dbt_dry_run.models import DryRunStatus, Table


class SQLRunner(metaclass=ABCMeta):
    """
    Used to adapt to multiple warehouse backends
    """

    @abstractmethod
    def close(self) -> None:
        ...

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
