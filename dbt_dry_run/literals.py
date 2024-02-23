import re
from typing import Callable, Dict, cast
from uuid import uuid4

from dbt_dry_run.exception import UpstreamFailedException
from dbt_dry_run.models import BigQueryFieldMode, BigQueryFieldType, Table, TableField
from dbt_dry_run.models.manifest import Node
from dbt_dry_run.results import DryRunStatus, Results

_EXAMPLE_VALUES: Dict[BigQueryFieldType, Callable[[], str]] = {
    BigQueryFieldType.STRING: lambda: f"'{uuid4()}'",
    BigQueryFieldType.BYTES: lambda: f"b'{uuid4()}'",
    BigQueryFieldType.INTEGER: lambda: "1",
    BigQueryFieldType.INT64: lambda: "1",
    BigQueryFieldType.FLOAT: lambda: "1.0",
    BigQueryFieldType.FLOAT64: lambda: "1.0",
    BigQueryFieldType.BOOLEAN: lambda: "true",
    BigQueryFieldType.BOOL: lambda: "true",
    BigQueryFieldType.TIMESTAMP: lambda: "TIMESTAMP('2021-01-01')",
    BigQueryFieldType.DATE: lambda: "DATE('2021-01-01')",
    BigQueryFieldType.TIME: lambda: "TIME(12,0,0)",
    BigQueryFieldType.DATETIME: lambda: "DATETIME(2021,1,1,12,0,0)",
    BigQueryFieldType.GEOGRAPHY: lambda: "ST_GeogPoint(0.0, 0.0)",
    BigQueryFieldType.INTERVAL: lambda: "MAKE_INTERVAL(1)",
    BigQueryFieldType.NUMERIC: lambda: "CAST(1 AS NUMERIC)",
    BigQueryFieldType.BIGNUMERIC: lambda: "CAST(2 AS BIGNUMERIC)",
    BigQueryFieldType.JSON: lambda: "PARSE_JSON('{\"a\": 1}')",
}

_EXAMPLE_VALUES_TEST: Dict[BigQueryFieldType, Callable[[], str]] = {
    BigQueryFieldType.STRING: lambda: f"'foo'",
    BigQueryFieldType.BYTES: lambda: f"b'foo'",
    BigQueryFieldType.INTEGER: lambda: "1",
    BigQueryFieldType.INT64: lambda: "1",
    BigQueryFieldType.FLOAT: lambda: "1.0",
    BigQueryFieldType.FLOAT64: lambda: "1.0",
    BigQueryFieldType.BOOLEAN: lambda: "true",
    BigQueryFieldType.BOOL: lambda: "true",
    BigQueryFieldType.TIMESTAMP: lambda: "TIMESTAMP('2021-01-01')",
    BigQueryFieldType.DATE: lambda: "DATE('2021-01-01')",
    BigQueryFieldType.TIME: lambda: "TIME(12,0,0)",
    BigQueryFieldType.DATETIME: lambda: "DATETIME(2021,1,1,12,0,0)",
    BigQueryFieldType.INTERVAL: lambda: "MAKE_INTERVAL(1)",
    BigQueryFieldType.GEOGRAPHY: lambda: "ST_GeogPoint(0.0, 0.0)",
    BigQueryFieldType.NUMERIC: lambda: "CAST(1 AS NUMERIC)",
    BigQueryFieldType.BIGNUMERIC: lambda: "CAST(2 AS BIGNUMERIC)",
    BigQueryFieldType.JSON: lambda: "PARSE_JSON('{\"a\": 1}')",
}

_ACTIVE_EXAMPLE_VALUES = _EXAMPLE_VALUES


def enable_test_example_values(enabled: bool) -> None:
    global _ACTIVE_EXAMPLE_VALUES
    if enabled:
        _ACTIVE_EXAMPLE_VALUES = _EXAMPLE_VALUES_TEST
    else:
        _ACTIVE_EXAMPLE_VALUES = _EXAMPLE_VALUES


def get_example_value(type_: BigQueryFieldType) -> str:
    return _ACTIVE_EXAMPLE_VALUES[type_]()


def get_sql_literal_from_field(field: TableField) -> str:
    is_repeated = field.mode == BigQueryFieldMode.REPEATED
    is_complex = field.type_ in (BigQueryFieldType.RECORD, BigQueryFieldType.STRUCT)
    if is_complex and field.fields:
        complex_dummies = map(get_sql_literal_from_field, field.fields)
        dummy_value = f"STRUCT({','.join(complex_dummies)})"
    else:
        dummy_value = get_example_value(field.type_)
    if is_repeated:
        dummy_value = f"[{dummy_value}]"
    statement = f"{dummy_value} as `{field.name}`"

    return statement


def get_sql_literal_from_table(table: Table) -> str:
    literal_fields = ",".join(map(get_sql_literal_from_field, table.fields))
    select_literal = f"(SELECT {literal_fields})"
    return select_literal


def replace_upstream_sql(node_sql: str, node: Node, table: Table) -> str:
    upstream_table_ref = node.to_table_ref_literal()
    regex = re.compile(
        rf"((?:from|join)(?:\s--.*)?[\r\n\s]*)({upstream_table_ref})",
        flags=re.IGNORECASE | re.MULTILINE,
    )
    select_literal = get_sql_literal_from_table(table)
    new_node_sql = regex.sub(r"\1" + select_literal, node_sql)
    return new_node_sql


def insert_dependant_sql_literals(node: Node, results: Results) -> str:
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
            node_new_sql, upstream.node, cast(Table, upstream.table)
        )
    return node_new_sql
