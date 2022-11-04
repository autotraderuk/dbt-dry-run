
from typing import Optional, Tuple, Any
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from psycopg2.extensions import connection as Connection
from dbt_dry_run.adapter.service import ProjectService
from dbt_dry_run.models import Table, TableField
from dbt_dry_run.models.manifest import Node
from dbt_dry_run.results import DryRunStatus
from dbt_dry_run.sql_runner import SQLRunner

MAX_ATTEMPT_NUMBER = 5
QUERY_TIMED_OUT = "Dry run query timed out"


class PostgresSQLRunner(SQLRunner):

    def __init__(self, project: ProjectService):
        self._project = project

    def node_exists(self, node: Node) -> bool:
        return self.get_node_schema(node) is not None

    def get_node_schema(self, node: Node) -> Optional[Table]:
        client = self.get_client()
        return None

    def get_client(self) -> Connection:
        connection = self._project.get_connection()
        return connection.handle

    def query(
        self, sql: str
    ) -> Tuple[DryRunStatus, Optional[Table], Optional[Exception]]:
        exception = None
        table = None
        client = self.get_client()
        cur = client.cursor()
        cur.execute(sql)
        first_row = cur.fetchone()
        table = self.get_schema_from_query_job(first_row)
        status = DryRunStatus.SUCCESS
        return status, table, exception

    @staticmethod
    def get_schema_from_query_job(query_job: Any) -> Table:
        job_fields = []
        return Table(fields=job_fields)
