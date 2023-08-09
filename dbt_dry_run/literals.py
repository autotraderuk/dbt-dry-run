import re
from typing import cast

from dbt_dry_run.exception import UpstreamFailedException
from dbt_dry_run.models import Table
from dbt_dry_run.models.manifest import Node
from dbt_dry_run.results import DryRunStatus, Results
from dbt_dry_run.sql_runner import SQLRunner


def get_sql_literal_from_table(literal_name: str, table: Table, sql_runner: SQLRunner) -> str:
    literal_fields = ",".join(map(lambda field: sql_runner.get_sql_literal_from_field(field), table.fields))
    select_literal = f"(SELECT {literal_fields}) {literal_name}"
    return select_literal


def replace_upstream_sql(node_sql: str, node: Node, table: Table, sql_runner: SQLRunner) -> str:
    upstream_table_ref = sql_runner.get_node_identifier(node)
    regex = re.compile(
        rf"((?:from|join)(?:\s--.*)?[\r\n\s]*)({upstream_table_ref})",
        flags=re.IGNORECASE | re.MULTILINE,
    )
    select_literal = get_sql_literal_from_table(node.alias, table, sql_runner)
    new_node_sql = regex.sub(r"\1" + select_literal, node_sql)
    return new_node_sql


def insert_dependant_sql_literals(node: Node, results: Results, sql_runner: SQLRunner) -> str:
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

    node_new_sql = node.compiled_code
    for upstream in completed_upstreams:
        node_new_sql = replace_upstream_sql(
            node_new_sql, upstream.node, cast(Table, upstream.table), sql_runner
        )
    return node_new_sql
