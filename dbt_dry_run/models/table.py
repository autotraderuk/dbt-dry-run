from enum import Enum
from typing import List, Optional, Set

import pydantic
from google.cloud.bigquery import SchemaField
from google.cloud.bigquery.table import Table as BigQueryTable
from pydantic import Field
from pydantic.main import BaseModel


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
    INTERVAL = "INTERVAL"
    GEOGRAPHY = "GEOGRAPHY"
    NUMERIC = "NUMERIC"
    BIGNUMERIC = "BIGNUMERIC"
    STRUCT = "STRUCT"
    JSON = "JSON"
    RECORD = "RECORD"


class TableField(BaseModel):
    name: str
    type_: BigQueryFieldType = Field(..., alias="type")
    mode: Optional[BigQueryFieldMode]
    fields: Optional[List["TableField"]] = None
    description: Optional[str]

    @pydantic.validator("type_", pre=True)
    def validate_type_field(cls, field: str) -> BigQueryFieldType:
        return BigQueryFieldType(field)


TableField.update_forward_refs()


class Table(BaseModel):
    fields: List[TableField]

    @classmethod
    def from_bigquery_table(cls, bigquery_table: BigQueryTable) -> "Table":
        schema = bigquery_table.schema
        new_fields = cls.map_fields(schema)
        return cls(fields=new_fields)

    @property
    def field_names(self) -> Set[str]:
        return set(field.name for field in self.fields)

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
