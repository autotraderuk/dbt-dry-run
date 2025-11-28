import json
from enum import Enum
from pathlib import Path
from typing import Any, ClassVar, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, RootModel, model_validator

from dbt_dry_run import flags


class SnapshotMetaColumnName(str, Enum):
    DBT_VALID_FROM = "dbt_valid_from"
    DBT_VALID_TO = "dbt_valid_to"
    DBT_SCD_ID = "dbt_scd_id"
    DBT_UPDATED_AT = "dbt_updated_at"
    DBT_IS_DELETED = "dbt_is_deleted"


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


class TableRef(BaseModel):
    database: str
    db_schema: str
    name: str

    @property
    def bq_literal(self) -> str:
        return f"`{self.database}`.`{self.db_schema}`.`{self.name}`"


class PartitionBy(BaseModel):
    field: str
    data_type: Literal["timestamp", "date", "datetime", "int64"]
    range: Optional[IntPartitionRange] = None
    time_ingestion_partitioning: Optional[bool] = None

    @model_validator(mode="before")
    def lower_data_type(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        values["data_type"] = values["data_type"].lower()
        return values


class NodeMeta(RootModel[Dict[str, Any]]):
    DEFAULT_CHECK_COLUMNS_KEY: ClassVar[str] = "dry_run.check_columns"

    def get(self, key: str) -> Optional[Any]:
        return self.root.get(key)

    def __getitem__(self, key: str) -> Any:
        try:
            return self.root[key]
        except KeyError:
            raise KeyError(f"Node does not have metadata '{key}'")

    def __contains__(self, key: str) -> bool:
        return key in self.root

    model_config = ConfigDict(populate_by_name=True)


class NodeConfig(BaseModel):
    enabled: bool = True
    materialized: Optional[str] = None
    on_schema_change: Optional[OnSchemaChange] = None
    sql_header: Optional[str] = None
    unique_key: Optional[Union[str, List[str]]] = None
    updated_at: Optional[str] = None
    strategy: Union[None, Literal["timestamp", "check"]] = None
    check_cols: Optional[Union[Literal["all"], List[str]]] = None
    partition_by: Optional[PartitionBy] = None
    meta: Optional[NodeMeta] = None
    full_refresh: Optional[bool] = None
    column_types: Dict[str, str] = Field(default_factory=dict)
    delimiter: Optional[str] = None
    hard_deletes: Optional[Literal["ignore", "invalidate", "new_record"]] = None


class ManifestColumn(BaseModel):
    name: str
    description: Optional[str] = None
    data_type: Optional[str] = None


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
    db_schema: str = Field(..., alias="schema", serialization_alias="schema")
    alias: str
    language: Optional[str] = None
    resource_type: str
    original_file_path: str
    root_path: Optional[str] = None
    columns: Dict[str, ManifestColumn] = Field(default_factory=dict)
    meta: Optional[NodeMeta] = None
    external: Optional[ExternalConfig] = None

    @model_validator(mode="before")
    def default_alias(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        values["alias"] = values.get("alias") or values["name"]
        return values

    @property
    def table_ref(self) -> TableRef:
        if self.alias:
            name_param = self.alias
        else:
            name_param = self.name
        return TableRef(
            database=self.database,
            db_schema=self.db_schema,
            name=name_param,
        )

    def get_table_ref_literal(self) -> str:
        return self.table_ref.bq_literal

    def get_combined_metadata(self, key: str) -> Optional[Any]:
        node_meta = self.meta.get(key) if self.meta else None
        config_meta = self.config.meta.get(key) if self.config.meta else None
        merged_meta = config_meta if config_meta is not None else node_meta
        return merged_meta

    def is_external_source(self) -> bool:
        return self.external is not None and self.resource_type == "source"

    @property
    def is_seed(self) -> bool:
        return self.resource_type == "seed"

    def get_should_full_refresh(self) -> bool:
        # precedence defined here - https://docs.getdbt.com/reference/resource-configs/full_refresh
        if self.config.full_refresh is not None:
            return self.config.full_refresh
        return flags.FULL_REFRESH

    @property
    def is_time_ingestion_partitioned(self) -> bool:
        if self.config.partition_by:
            if self.config.partition_by.time_ingestion_partitioning is True:
                return True
        return False


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
