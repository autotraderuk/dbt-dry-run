from typing import Callable, Dict, List, cast

from dbt_dry_run.exception import UpstreamFailedException
from dbt_dry_run.models import Table
from dbt_dry_run.models.manifest import Node
from dbt_dry_run.models.report import DryRunStatus
from dbt_dry_run.results import Results
from dbt_dry_run.sql.literals import replace_upstream_sql

PARTITION_DATA_TYPES_VALUES_MAPPING: Dict[str, str] = {
    "timestamp": "CURRENT_TIMESTAMP()",
    "datetime": "CURRENT_DATETIME()",
    "date": "CURRENT_DATE()",
    "int64": "100",
}

SQLPreprocessorStep = Callable[[str, Node, Results], str]


class SQLPreprocessor:
    def __init__(self, transformers: List[SQLPreprocessorStep]):
        self.transformers = transformers

    def __call__(self, node: Node, results: Results) -> str:
        sql_statement = node.compiled_code
        for transformer in self.transformers:
            sql_statement = transformer(sql_statement, node, results)
        return sql_statement


def insert_dependant_sql_literals(
    sql_statement: str, node: Node, results: Results
) -> str:
    if node.depends_on.deep_nodes is not None:
        upstream_results = [
            results.get_result(n)
            for n in node.depends_on.deep_nodes
            if n in results.keys()
        ]
    else:
        raise KeyError(f"deep_nodes have not been created for {node.unique_id}")
    failed_upstreams = [r for r in upstream_results if r.status != DryRunStatus.SUCCESS]
    if failed_upstreams:
        failed_upstreams_messages = ", ".join(
            [f"{f.node.unique_id} : {f.status}" for f in failed_upstreams]
        )
        msg = f"Can't insert SELECT literals for {node.unique_id}. Upstreams did not run with status: {failed_upstreams_messages}"
        raise UpstreamFailedException(msg)
    completed_upstreams = [r for r in upstream_results if r.table]

    node_new_sql = sql_statement
    for upstream in completed_upstreams:
        node_new_sql = replace_upstream_sql(
            node_new_sql, upstream.node, cast(Table, upstream.table)
        )
    return node_new_sql


def create_or_replace_view(sql_statement: str, node: Node, _: Results) -> str:
    sql_statement = (
        f"CREATE OR REPLACE VIEW {node.table_ref.bq_literal} AS (\n{sql_statement}\n)"
    )
    return sql_statement


def add_sql_header(sql_statement: str, node: Node, _: Results) -> str:
    if node.config.sql_header:
        sql_statement = f"{node.config.sql_header}\n{sql_statement}"
    return sql_statement


def add_dbt_max_partition_declaration(
    sql_statement: str, node: Node, _: Results
) -> str:
    if node.config.partition_by and "_dbt_max_partition" in node.compiled_code:
        dbt_max_partition_declaration = (
            f"declare _dbt_max_partition {node.config.partition_by.data_type} default"
            f" {PARTITION_DATA_TYPES_VALUES_MAPPING[node.config.partition_by.data_type]};"
        )
        sql_statement = f"{dbt_max_partition_declaration}\n{sql_statement}"
    return sql_statement
