from typing import Any, Callable, List, Optional, Tuple

import agate
from google.cloud.bigquery import (
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
from dbt_dry_run.models import Table, TableField
from dbt_dry_run.models.manifest import Node
from dbt_dry_run.models.report import DryRunStatus
from dbt_dry_run.sql_runner import SQLRunner

MAX_ATTEMPT_NUMBER = 5
QUERY_TIMED_OUT = "Dry run query timed out"


class Integer(agate.data_types.DataType):  # type: ignore
    def cast(self, d: Any) -> Optional[int]:
        # by default agate will cast none as a Number
        # but we need to cast it as an Integer to preserve
        # the type when merging and unioning tables
        if type(d) == int or d is None:  # noqa [E721]
            return d
        else:
            raise agate.exceptions.CastError('Can not parse value "%s" as Integer.' % d)

    def jsonify(self, d: Optional[int]) -> Optional[int]:
        return d


class BigQuerySQLRunner(SQLRunner):
    JOB_CONFIG = QueryJobConfig(dry_run=True, use_query_cache=False)

    def __init__(self, project: ProjectService):
        self._project = project

    def node_exists(self, node: Node) -> bool:
        return self.get_node_schema(node) is not None

    def get_node_schema(self, node: Node) -> Optional[Table]:
        client = self._project.get_client()
        try:
            dataset = DatasetReference(node.database, node.db_schema)
            table_ref = TableReference(dataset, node.alias)
            bigquery_table = client.get_table(table_ref)

            return Table.from_bigquery_table(bigquery_table)
        except NotFound:
            return None

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
        client = self._project.get_client()
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
                    schema_field, e
                ) from e

        job_fields = list(map(_map_schema_fields_to_table_field, schema_fields))
        return Table(fields=job_fields)

    @classmethod
    def convert_agate_type(
        cls, agate_table: agate.Table, col_idx: int
    ) -> Optional[str]:
        agate_type = agate_table.column_types[col_idx]
        conversions: List[Tuple[Any, Callable[..., str]]] = [
            (Integer, cls.convert_integer_type),
            (agate.Text, cls.convert_text_type),
            (agate.Number, cls.convert_number_type),
            (agate.Boolean, cls.convert_boolean_type),
            (agate.DateTime, cls.convert_datetime_type),
            (agate.Date, cls.convert_date_type),
            (agate.TimeDelta, cls.convert_time_type),
        ]
        for agate_cls, func in conversions:
            if isinstance(agate_type, agate_cls):
                return func(agate_table, col_idx)
        return None

    @classmethod
    def convert_text_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        return "string"

    @classmethod
    def convert_number_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        import agate

        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))
        return "float64" if decimals else "int64"

    @classmethod
    def convert_integer_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        return "int64"

    @classmethod
    def convert_boolean_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        return "bool"

    @classmethod
    def convert_datetime_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        return "datetime"

    @classmethod
    def convert_date_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        return "date"

    @classmethod
    def convert_time_type(cls, agate_table: "agate.Table", col_idx: int) -> str:
        return "time"
