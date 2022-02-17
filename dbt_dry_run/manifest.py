import json
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class NodeDependsOn(BaseModel):
    macros: List[str]
    nodes: List[str]
    deep_nodes: Optional[List[str]] = None


class OnSchemaChange(str, Enum):
    APPEND_NEW_COLUMNS = "append_new_columns"
    FAIL = "fail"
    IGNORE = "ignore"
    SYNC_ALL_COLUMNS = "sync_all_columns"


class NodeConfig(BaseModel):
    materialized: str
    on_schema_change: Optional[OnSchemaChange]
    sql_header: Optional[str]


class Node(BaseModel):
    name: str
    config: NodeConfig
    unique_id: str
    depends_on: NodeDependsOn
    compiled: bool = False
    compiled_sql: str = ""
    database: str
    db_schema: str = Field(..., alias="schema")
    alias: str
    resource_type: str
    original_file_path: str
    root_path: str

    def to_table_ref_literal(self) -> str:
        sql = f"`{self.database}`.`{self.db_schema}`.`{self.alias}`"
        return sql


class Macro(BaseModel):
    root_path: Path
    original_file_path: Path


class Manifest(BaseModel):
    nodes: Dict[str, Node]
    macros: Dict[str, Macro]


def read_manifest(path: str) -> Manifest:
    with open(path) as fh:
        data = json.load(fh)
    m = Manifest(**data)
    return m
