from typing import List, Optional, Tuple, Dict, Callable
from uuid import uuid4

from google.cloud.bigquery import (
    Client,
    DatasetReference,
    QueryJobConfig,
    SchemaField,
    TableReference,
)
from google.cloud.exceptions import BadRequest, Forbidden, NotFound
from pydantic import ValidationError
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from dbt_dry_run.adapter.service import ProjectService
from dbt_dry_run.exception import UnknownSchemaException
from dbt_dry_run.models import Table, TableField, FieldMode, FieldType
from dbt_dry_run.models.manifest import Node
from dbt_dry_run.results import DryRunStatus
from dbt_dry_run.sql_runner import SQLRunner

MAX_ATTEMPT_NUMBER = 5
QUERY_TIMED_OUT = "Dry run query timed out"

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


class BigQuerySQLRunner(SQLRunner):
    JOB_CONFIG = QueryJobConfig(dry_run=True, use_query_cache=False)

    def __init__(self, project: ProjectService):
        self._project = project

    def node_exists(self, node: Node) -> bool:
        return self.get_node_schema(node) is not None

    def get_node_schema(self, node: Node) -> Optional[Table]:
        client = self.get_client()
        try:
            dataset = DatasetReference(node.database, node.db_schema)
            table_ref = TableReference(dataset, node.alias)
            bigquery_table = client.get_table(table_ref)

            return Table.from_bigquery_table(bigquery_table)
        except NotFound:
            return None

    def get_client(self) -> Client:
        connection = self._project.get_connection()
        return connection.handle

    def get_node_identifier(self, node: Node) -> str:
        return f"`{node.database}`.`{node.db_schema}`.`{node.alias}`"

    def get_sql_literal_from_field(self, field: TableField) -> str:
        is_repeated = field.mode == FieldMode.REPEATED
        is_complex = field.type_ in (FieldType.RECORD, FieldType.STRUCT)
        if is_complex and field.fields:
            complex_dummies = map(lambda field_: self.get_sql_literal_from_field(field_), field.fields)
            dummy_value = f"STRUCT({','.join(complex_dummies)})"
        else:
            dummy_value = self.get_example_value(field.type_)
        if is_repeated:
            dummy_value = f"[{dummy_value}]"
        statement = f"{dummy_value} as `{field.name}`"

        return statement

    def get_example_value(self, type_: FieldType) -> str:
        return _ACTIVE_EXAMPLE_VALUES[type_]()

    @retry(
        retry=retry_if_exception_type(BadRequest),
        stop=stop_after_attempt(MAX_ATTEMPT_NUMBER),
        wait=wait_exponential(multiplier=0.5, min=0.5, max=10),
    )
    def query(
            self, sql: str
    ) -> Tuple[DryRunStatus, Optional[Table], Optional[Exception]]:
        exception = None
        table = None
        client = self.get_client()
        try:
            query_job = client.query(sql, job_config=self.JOB_CONFIG)
            table = self.get_schema_from_schema_fields(query_job.schema or [])
            status = DryRunStatus.SUCCESS
        except (Forbidden, BadRequest, NotFound) as e:
            status = DryRunStatus.FAILURE
            if QUERY_TIMED_OUT in str(e):
                raise
            exception = e
        return status, table, exception

    @staticmethod
    def get_schema_from_schema_fields(schema_fields: List[SchemaField]) -> Table:
        def _map_schema_fields_to_table_field(schema_field: SchemaField) -> TableField:
            try:
                parsed_fields = (
                    BigQuerySQLRunner.get_schema_from_schema_fields(
                        schema_field.fields
                    ).fields
                    if schema_field.fields
                    else None
                )
                return TableField(
                    name=schema_field.name,
                    mode=schema_field.mode,
                    type=schema_field.field_type,
                    description=schema_field.description,
                    fields=parsed_fields,
                )
            except ValidationError as e:
                raise UnknownSchemaException.from_validation_error(
                    schema_field.name, e
                ) from e

        job_fields = list(map(_map_schema_fields_to_table_field, schema_fields))
        return Table(fields=job_fields)
