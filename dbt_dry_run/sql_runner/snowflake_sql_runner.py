from typing import List, Optional, Tuple

from pydantic import ValidationError
from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import ResultMetadata

from dbt_dry_run.adapter.service import ProjectService
from dbt_dry_run.exception import UnknownSchemaException
from dbt_dry_run.models import BigQueryFieldMode, Table, TableField
from dbt_dry_run.models.manifest import Node
from dbt_dry_run.results import DryRunStatus
from dbt_dry_run.sql_runner import SQLRunner

MAX_ATTEMPT_NUMBER = 5
QUERY_TIMED_OUT = "Dry run query timed out"

# https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-api#type-codes
TYPE_CODE_MAPPING = {
    0: "NUMBER",
    1: "REAL",
    2: "TEXT",
    3: "DATE",
    4: "TIMESTAMP",
    5: "VARIANT",
    6: "TIMESTAMP_LTZ",
    7: "TIMESTAMP_TZ",
    8: "TIMESTAMP_NTZ",
    9: "OBJECT",
    10: "ARRAY",
    11: "BINARY",
    12: "TIME",
    13: "BOOLEAN",
}


class SnowflakeSQLRunner(SQLRunner):
    def __init__(self, project: ProjectService):
        self._project = project

    def node_exists(self, node: Node) -> bool:
        return self.get_node_schema(node) is not None

    def get_node_schema(self, node: Node) -> Optional[Table]:
        raise NotImplementedError("Not done yet")

    def get_client(self) -> SnowflakeConnection:
        connection = self._project.get_connection()
        return connection.handle

    def get_node_identifier(self, node: Node) -> str:
        return node.alias

    def query(
        self, sql: str
    ) -> Tuple[DryRunStatus, Optional[Table], Optional[Exception]]:
        exception = None
        table = None
        client = self.get_client()
        try:
            cur = client.cursor()
            print(f"Running:\n{sql}")
            cur.execute(sql, _describe_only=True)
            table = self.get_schema_from_schema_fields(cur.description)
            status = DryRunStatus.SUCCESS
        except (Exception) as e:
            status = DryRunStatus.FAILURE
            exception = e
        return status, table, exception

    @staticmethod
    def get_schema_from_schema_fields(schema_fields: List[ResultMetadata]) -> Table:
        def _map_schema_fields_to_table_field(
            schema_field: ResultMetadata,
        ) -> TableField:
            try:
                if schema_field.is_nullable:
                    mode = BigQueryFieldMode.NULLABLE
                else:
                    mode = BigQueryFieldMode.REQUIRED
                return TableField(
                    name=schema_field.name,
                    mode=mode,
                    type=schema_field.type_code,
                    description=None,
                    fields=None,
                )
            except ValidationError as e:
                raise UnknownSchemaException.from_validation_error(
                    schema_field.name, e
                ) from e

        job_fields = list(map(_map_schema_fields_to_table_field, schema_fields))
        return Table(fields=job_fields)
