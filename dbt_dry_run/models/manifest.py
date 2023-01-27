import json
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, Field, root_validator


class NodeDependsOn(BaseModel):
    macros: List[str] = []
    nodes: List[str] = []
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
    enabled: bool = True
    materialized: Optional[str]
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
    data_type: Optional[str]


class ExternalConfig(BaseModel):
    location: str
    dry_run_columns: List[ManifestColumn] = Field(default_factory=lambda: list())

    @property
    def dry_run_columns_map(self) -> Dict[str, ManifestColumn]:
        return {c.name: c for c in self.dry_run_columns}


class Node(BaseModel):
    name: str
    config: NodeConfig
    unique_id: str
    depends_on: NodeDependsOn = NodeDependsOn()
    compiled: bool = False
    compiled_code: str = ""
    database: str
    db_schema: str = Field(..., alias="schema")
    alias: str
    language: Optional[str] = None
    resource_type: str
    original_file_path: str
    root_path: Optional[str] = None
    columns: Dict[str, ManifestColumn]
    meta: Optional[NodeMeta]
    external: Optional[ExternalConfig]

    def __init__(self, **data: Any):
        super().__init__(
            compiled_code=data.pop("compiled_code", "") or data.pop("compiled_sql", ""),
            **data,
        )

    @root_validator(pre=True)
    def default_alias(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        values["alias"] = values.get("alias") or values["name"]
        return values

    def to_table_ref_literal(self) -> str:
        if self.alias:
            sql = f"`{self.database}`.`{self.db_schema}`.`{self.alias}`"
        else:
            sql = f"`{self.database}`.`{self.db_schema}`.`{self.name}`"
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

    def is_external_source(self) -> bool:
        return self.external is not None and self.resource_type == "source"

    @property
    def is_seed(self) -> bool:
        return self.resource_type == "seed"


class Macro(BaseModel):
    original_file_path: Path


class Manifest(BaseModel):
    nodes: Dict[str, Node]
    sources: Dict[str, Node]
    macros: Dict[str, Macro]

    @property
    def all_nodes(self) -> Dict[str, Node]:
        return dict(self.nodes, **self.sources)

    @classmethod
    def from_filepath(cls, path: str) -> "Manifest":
        try:
            with open(path) as fh:
                data = json.load(fh)
        except FileNotFoundError:
            raise FileNotFoundError(f"Incorrect Manifest filepath: '{path}'")
        m = Manifest(**data)
        return m
