from typing import Callable, Dict, List, Optional
from uuid import uuid4

import sqlglot

from dbt_dry_run.exception import UpstreamFailedException
from dbt_dry_run.models import BigQueryFieldMode, BigQueryFieldType, Table, TableField
from dbt_dry_run.models.manifest import Node
from dbt_dry_run.results import DryRunResult, DryRunStatus, Results

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
    BigQueryFieldType.RANGE: lambda: "RANGE(DATE '2022-12-01', DATE '2022-12-31')",
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
    BigQueryFieldType.RANGE: lambda: "RANGE(DATE '2022-12-01', DATE '2022-12-31')",
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


def convert_ast_to_sql(trees: List[sqlglot.Expression]) -> str:
    return ";\n".join(tree.sql(sqlglot.dialects.BigQuery) for tree in trees)


def _table_from_node(node: Node) -> sqlglot.Expression:
    return sqlglot.exp.table_(
        catalog=node.database, db=node.db_schema, table=node.alias, quoted=True
    )


def _remove_alias_from_table(exp: sqlglot.Expression) -> Optional[sqlglot.Expression]:
    if isinstance(exp, sqlglot.exp.TableAlias):
        return None
    return exp


def replace_upstream_sql(node_sql: str, upstream_results: List[DryRunResult]) -> str:
    parsed_statements = sqlglot.parse(node_sql, dialect=sqlglot.dialects.BigQuery)
    upstream_literals = {
        _table_from_node(upstream.node): get_sql_literal_from_table(upstream.table)
        for upstream in upstream_results
        if upstream.table
    }

    def transformer(exp: sqlglot.Expression) -> sqlglot.Expression:
        if isinstance(exp, sqlglot.exp.Table):
            table_without_alias = exp.transform(_remove_alias_from_table)
            literal = upstream_literals.get(table_without_alias)
            if literal:
                new_alias = exp.alias or exp.name
                return sqlglot.parse_one(
                    literal, dialect=sqlglot.dialects.BigQuery
                ).as_(new_alias)
        return exp

    transformed_trees = [
        parsed.transform(transformer) for parsed in parsed_statements if parsed
    ]
    return convert_ast_to_sql(transformed_trees)


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

    node_new_sql = replace_upstream_sql(node.compiled_code, completed_upstreams)
    return node_new_sql
