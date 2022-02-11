from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery.table import Table as BigQueryTable
from pydantic import BaseModel, Field, validator

from dbt_dry_run.manifest import Node


class BigQueryFieldMode(str, Enum):
    NULLABLE = "NULLABLE"
    REQUIRED = "REQUIRED"
    REPEATED = "REPEATED"


class BigQueryFieldType(str, Enum):
    STRING = "STRING"
    BYTES = "BYTES"
    INTEGER = "INTEGER"
    INT64 = "INT64"
    FLOAT = "FLOAT"
    FLOAT64 = "FLOAT64"
    BOOLEAN = "BOOLEAN"
    BOOL = "BOOL"
    TIMESTAMP = "TIMESTAMP"
    DATE = "DATE"
    TIME = "TIME"
    DATETIME = "DATETIME"
    GEOGRAPHY = "GEOGRAPHY"
    NUMERIC = "NUMERIC"
    BIGNUMERIC = "BIGNUMERIC"
    RECORD = "RECORD"
    STRUCT = "STRUCT"


class TableField(BaseModel):
    name: str
    type_: BigQueryFieldType = Field(..., alias="type")
    mode: Optional[BigQueryFieldMode]
    fields: Optional[List["TableField"]]
    description: Optional[str]


TableField.update_forward_refs()


class Table(BaseModel):
    fields: List[TableField]

    @classmethod
    def from_bigquery_table(cls, bigquery_table: BigQueryTable) -> "Table":
        schema = bigquery_table.schema
        new_fields = cls.map_fields(schema)
        return cls(fields=new_fields)

    @classmethod
    def map_fields(
        cls, schema: Optional[List[SchemaField]]
    ) -> Optional[List[TableField]]:
        new_fields = []

        if schema is None:
            return None

        for field in schema:
            table_field = TableField(
                name=field.name,
                type=field.field_type,
                mode=field.mode,
                fields=cls.map_fields(field.fields),
                description=field.description,
            )
            new_fields.append(table_field)
        return new_fields


class DryRunStatus(str, Enum):
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"


@dataclass(frozen=True)
class DryRunResult:
    node: Node
    table: Optional[Table]
    status: DryRunStatus
    exception: Optional[Exception]

    def replace_table(self, table: Table) -> "DryRunResult":
        return DryRunResult(
            node=self.node, table=table, status=self.status, exception=self.exception
        )


class BigQueryConnectionMethod(str, Enum):
    OAUTH = "oauth"
    SERVICE_ACCOUNT = "service-account"


class Output(BaseModel):
    output_type: str = Field(..., alias="type")
    method: BigQueryConnectionMethod
    project: str
    db_schema: str = Field(..., alias="schema")
    location: str
    threads: int = Field(..., ge=1)
    timeout_seconds: float = Field(..., ge=0)
    keyfile: Path
    scopes: List[str] = []


class Profile(BaseModel):
    outputs: Dict[str, Output]
    target: str

    @validator("target")
    def target_must_be_valid_output(
        cls, target: str, values: Dict[str, Any], **kwargs: Dict[str, Any]
    ) -> str:
        output_keys = set(values["outputs"].keys())
        if target not in output_keys:
            raise ValueError(
                f"target={target} but it must be valid output={output_keys}"
            )
        return target
