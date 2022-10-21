import json
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, Field, root_validator


class NodeDependsOn(BaseModel):
    macros: List[str]
    nodes: List[str]
    deep_nodes: Optional[List[str]] = None


class OnSchemaChange(str, Enum):
    APPEND_NEW_COLUMNS = "append_new_columns"
    FAIL = "fail"
    IGNORE = "ignore"
    SYNC_ALL_COLUMNS = "sync_all_columns"


class IntPartitionRange(BaseModel):
    start: int
    end: int
    interval: int


class PartitionBy(BaseModel):
    field: str
    data_type: Literal["timestamp", "date", "datetime", "int64"]
    range: Optional[IntPartitionRange]

    @root_validator(pre=True)
    def lower_data_type(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        values["data_type"] = values["data_type"].lower()
        return values


class NodeMeta(BaseModel):
    check_columns: bool = Field(False, alias="dry_run.check_columns")

    class Config:
        allow_population_by_field_name = True


class NodeConfig(BaseModel):
    materialized: str
    on_schema_change: Optional[OnSchemaChange]
    sql_header: Optional[str]
    unique_key: Optional[Union[str, List[str]]]
    updated_at: Optional[str]
    strategy: Union[None, Literal["timestamp", "check"]]
    check_cols: Optional[Union[Literal["all"], List[str]]]
    partition_by: Optional[PartitionBy]
    meta: Optional[NodeMeta]


class ManifestColumn(BaseModel):
    name: str
    description: Optional[str]


class Node(BaseModel):
    name: str
    config: NodeConfig
    unique_id: str
    depends_on: NodeDependsOn
    compiled: bool = False
    compiled_code: str = ""
    database: str
    db_schema: str = Field(..., alias="schema")
    alias: str
    language: Optional[str] = None
    resource_type: str
    original_file_path: str
    root_path: str
    columns: Dict[str, ManifestColumn]
    meta: Optional[NodeMeta]

    def __init__(self, **data: Any):
        super().__init__(
            compiled_code=data.pop("compiled_code", "") or data.pop("compiled_sql", ""),
            **data,
        )

    def to_table_ref_literal(self) -> str:
        sql = f"`{self.database}`.`{self.db_schema}`.`{self.alias}`"
        return sql

    def get_should_check_columns(self) -> bool:
        node_check_columns: bool = self.meta.check_columns if self.meta else False
        config_check_columns: Optional[bool] = (
            self.config.meta.check_columns if self.config.meta else None
        )
        merged_check_columns = (
            config_check_columns
            if config_check_columns is not None
            else node_check_columns
        )
        return merged_check_columns


class Macro(BaseModel):
    root_path: Path
    original_file_path: Path


class Manifest(BaseModel):
    nodes: Dict[str, Node]
    macros: Dict[str, Macro]

    @classmethod
    def from_filepath(cls, path: str) -> "Manifest":
        try:
            with open(path) as fh:
                data = json.load(fh)
        except FileNotFoundError:
            raise FileNotFoundError(f"Incorrect Manifest filepath: '{path}'")
        m = Manifest(**data)
        return m
