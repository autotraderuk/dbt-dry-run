from abc import ABCMeta, abstractmethod
from typing import Optional, Tuple

from dbt_dry_run.models import Table, TableField, FieldType
from dbt_dry_run.models.manifest import Node
from dbt_dry_run.results import DryRunStatus


class SQLRunner(metaclass=ABCMeta):
    """
    Used to adapt to multiple warehouse backends
    """

    @abstractmethod
    def node_exists(self, node: Node) -> bool:
        ...

    @abstractmethod
    def get_node_identifier(self, node: Node) -> str:
        ...

    @abstractmethod
    def get_example_value(self, type_: FieldType) -> str:
        ...

    @abstractmethod
    def get_sql_literal_from_field(self, field: TableField) -> str:
        ...

    @abstractmethod
    def get_node_schema(self, node: Node) -> Optional[Table]:
        ...

    @abstractmethod
    def query(
            self, sql: str
    ) -> Tuple[DryRunStatus, Optional[Table], Optional[Exception]]:
        ...
