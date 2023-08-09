from typing import List, Optional, Tuple, Callable, Dict
from uuid import uuid4

from pydantic import ValidationError
from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import ResultMetadata

from dbt_dry_run.adapter.service import ProjectService
from dbt_dry_run.exception import UnknownSchemaException
from dbt_dry_run.models import FieldMode, Table, TableField, FieldType
from dbt_dry_run.models.manifest import Node
from dbt_dry_run.results import DryRunStatus
from dbt_dry_run.sql_runner import SQLRunner

MAX_ATTEMPT_NUMBER = 5
QUERY_TIMED_OUT = "Dry run query timed out"

# https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-api#type-codes
TYPE_CODE_MAPPING = {
    0: FieldType.NUMERIC,  # "NUMBER"
    1: FieldType.FLOAT,  # "REAL"
    2: FieldType.STRING,  # "TEXT"
    3: FieldType.DATE,  # "DATE"
    4: FieldType.DATETIME,  # "TIMESTAMP"
    5: FieldType.JSON,  # "VARIANT"
    6: FieldType.DATETIME,  # "TIMESTAMP_LTZ"
    7: FieldType.DATETIME,  # "TIMESTAMP_TZ"
    8: FieldType.DATETIME,  # "TIMESTAMP_NTZ"
    9: FieldType.JSON,  # "OBJECT"
    10: FieldType.STRING,  # "ARRAY"
    11: FieldType.BYTES,  # "BINARY"
    12: FieldType.TIME,  # "TIME"
    13: FieldType.BOOLEAN,  # "BOOLEAN"
}

_EXAMPLE_VALUES: Dict[FieldType, Callable[[], str]] = {
    FieldType.STRING: lambda: f"'{uuid4()}'",
    FieldType.BYTES: lambda: f"b'{uuid4()}'",
    FieldType.INTEGER: lambda: "1",
    FieldType.INT64: lambda: "1",
    FieldType.FLOAT: lambda: "1.0",
    FieldType.FLOAT64: lambda: "1.0",
    FieldType.BOOLEAN: lambda: "true",
    FieldType.BOOL: lambda: "true",
    FieldType.TIMESTAMP: lambda: "TIMESTAMP('2021-01-01')",
    FieldType.DATE: lambda: "DATE('2021-01-01')",
    FieldType.TIME: lambda: "TIME(12,0,0)",
    FieldType.DATETIME: lambda: "DATETIME(2021,1,1,12,0,0)",
    FieldType.GEOGRAPHY: lambda: "ST_GeogPoint(0.0, 0.0)",
    FieldType.INTERVAL: lambda: "MAKE_INTERVAL(1)",
    FieldType.NUMERIC: lambda: "CAST(1 AS NUMERIC)",
    FieldType.BIGNUMERIC: lambda: "CAST(2 AS BIGNUMERIC)",
    FieldType.JSON: lambda: "PARSE_JSON('{\"a\": 1}')",
}

_EXAMPLE_VALUES_TEST: Dict[FieldType, Callable[[], str]] = {
    FieldType.STRING: lambda: f"'foo'",
    FieldType.BYTES: lambda: f"b'foo'",
    FieldType.INTEGER: lambda: "1",
    FieldType.INT64: lambda: "1",
    FieldType.FLOAT: lambda: "1.0",
    FieldType.FLOAT64: lambda: "1.0",
    FieldType.BOOLEAN: lambda: "true",
    FieldType.BOOL: lambda: "true",
    FieldType.TIMESTAMP: lambda: "TIMESTAMP('2021-01-01')",
    FieldType.DATE: lambda: "DATE('2021-01-01')",
    FieldType.TIME: lambda: "TIME(12,0,0)",
    FieldType.DATETIME: lambda: "DATETIME(2021,1,1,12,0,0)",
    FieldType.INTERVAL: lambda: "MAKE_INTERVAL(1)",
    FieldType.GEOGRAPHY: lambda: "ST_GeogPoint(0.0, 0.0)",
    FieldType.NUMERIC: lambda: "CAST(1 AS NUMERIC)",
    FieldType.BIGNUMERIC: lambda: "CAST(2 AS BIGNUMERIC)",
    FieldType.JSON: lambda: "PARSE_JSON('{\"a\": 1}')",
}

_ACTIVE_EXAMPLE_VALUES = _EXAMPLE_VALUES


def enable_test_example_values(enabled: bool) -> None:
    global _ACTIVE_EXAMPLE_VALUES
    if enabled:
        _ACTIVE_EXAMPLE_VALUES = _EXAMPLE_VALUES_TEST
    else:
        _ACTIVE_EXAMPLE_VALUES = _EXAMPLE_VALUES


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
        return f"{node.database}.{node.db_schema}.{node.alias}"

    def get_sql_literal_from_field(self, field: TableField) -> str:
        dummy_value = self.get_example_value(field.type_)
        statement = f"{dummy_value} as \"{field.name}\""
        return statement

    def get_example_value(self, type_: FieldType) -> str:
        return _ACTIVE_EXAMPLE_VALUES[type_]()

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
                    mode = FieldMode.NULLABLE
                else:
                    mode = FieldMode.REQUIRED
                return TableField(
                    name=schema_field.name,
                    mode=mode,
                    type=TYPE_CODE_MAPPING[schema_field.type_code],
                    description=None,
                    fields=None,
                )
            except ValidationError as e:
                raise UnknownSchemaException.from_validation_error(
                    schema_field.name, e
                ) from e

        job_fields = list(map(_map_schema_fields_to_table_field, schema_fields))
        return Table(fields=job_fields)
