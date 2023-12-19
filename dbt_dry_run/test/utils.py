from typing import List, Optional, Union
from unittest.mock import MagicMock

import sqlglot
from pydantic import BaseModel, Field

from dbt_dry_run.literals import convert_trees_to_sql
from dbt_dry_run.models import BigQueryFieldMode, BigQueryFieldType, Table, TableField
from dbt_dry_run.models.manifest import Node, NodeConfig, NodeDependsOn, NodeMeta
from dbt_dry_run.results import DryRunResult, DryRunStatus
from dbt_dry_run.scheduler import ManifestScheduler

A_SQL_QUERY = "SELECT * FROM `foo`"


class SimpleNode(BaseModel):
    unique_id: str
    depends_on: List[Union["SimpleNode", Node]]
    resource_type: str = ManifestScheduler.MODEL
    table_config: NodeConfig = NodeConfig(
        enabled=True, materialized="table", on_schema_change="ignore"
    )
    database: str = "my_db"
    db_schema: str = Field("my_schema", alias="schema")
    compiled_code: str = A_SQL_QUERY
    original_file_path: str = f"test123.sql"
    root_path: str = "/home/"
    meta: Optional[NodeMeta]

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
            compiled_code=self.compiled_code,
            database=self.database,
            schema=self.db_schema,
            alias=self.unique_id,
            resource_type=resource_type,
            original_file_path=self.original_file_path,
            root_path=self.root_path,
            columns=dict(),
            meta=self.meta,
        )


SimpleNode.update_forward_refs()


def field_with_name(
    name: str,
    type_: BigQueryFieldType = BigQueryFieldType.STRING,
    mode: BigQueryFieldMode = BigQueryFieldMode.NULLABLE,
    fields: Optional[List[TableField]] = None,
) -> TableField:
    return TableField(name=name, type=type_, mode=mode, fields=fields)


def get_executed_sql(mock: MagicMock) -> str:
    call_args = mock.query.call_args_list
    assert len(call_args) == 1
    executed_sql = call_args[0].args[0]
    return executed_sql


def assert_result_has_table(expected: Table, actual: DryRunResult) -> None:
    assert actual.status == DryRunStatus.SUCCESS
    assert actual.table

    actual_field_names = set([field.name for field in actual.table.fields])
    expected_field_names = set([field.name for field in expected.fields])

    assert (
        actual_field_names == expected_field_names
    ), f"Actual field names: {actual_field_names} did not equal expected: {expected_field_names}"


def assert_ast_equivalent(expected: str, actual: str) -> None:
    expected_ast = [
        t for t in sqlglot.parse(expected, dialect=sqlglot.dialects.BigQuery) if t
    ]
    expected_statement = convert_trees_to_sql(expected_ast)
    actual_ast = [
        t for t in sqlglot.parse(actual, dialect=sqlglot.dialects.BigQuery) if t
    ]
    actual_statement = convert_trees_to_sql(actual_ast)
    assert (
        actual_statement == expected_statement
    ), f"AST Not Equivalent:\nExpected:{expected}\nActual:{actual}"
