from typing import List

from pydantic import BaseModel, Field

from dbt_dry_run.manifest import Node, NodeConfig, NodeDependsOn
from dbt_dry_run.scheduler import ManifestScheduler

A_SQL_QUERY = "SELECT * FROM `foo`"


class SimpleNode(BaseModel):
    unique_id: str
    depends_on: List["SimpleNode"]
    resource_type: str = ManifestScheduler.MODEL
    table_config: NodeConfig = NodeConfig(
        materialized="table", on_schema_change="ignore"
    )
    database: str = "my_db"
    db_schema: str = Field("my_schema", alias="schema")
    compiled_sql: str = A_SQL_QUERY
    original_file_path: str = f"test123.sql"
    root_path: str = "/home/"

    def to_node(self) -> Node:
        depends_on = NodeDependsOn(
            nodes=[n.unique_id for n in self.depends_on], macros=[]
        )
        resource_type = self.resource_type
        return Node(
            name=self.unique_id,
            config=self.table_config,
            unique_id=self.unique_id,
            depends_on=depends_on,
            compiled=True,
            compiled_sql=self.compiled_sql,
            database=self.database,
            schema=self.db_schema,
            alias=self.unique_id,
            resource_type=resource_type,
            original_file_path=self.original_file_path,
            root_path=self.root_path,
        )


SimpleNode.update_forward_refs()
