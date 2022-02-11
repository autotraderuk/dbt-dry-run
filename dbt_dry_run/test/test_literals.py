import re
from typing import List

import pytest

from dbt_dry_run.literals import (
    enable_test_example_values,
    get_sql_literal_from_table,
    replace_upstream_sql,
)
from dbt_dry_run.models import BigQueryFieldMode, BigQueryFieldType, Table, TableField
from dbt_dry_run.test.utils import SimpleNode

enable_test_example_values(True)


def assert_fields_result_in_literal(fields: List[TableField], expected: str) -> None:
    actual = get_sql_literal_from_table(Table(fields=fields))
    assert (
        actual == expected
    ), f"SQL Literal:\n {actual} does not equal expected:\n {expected}"


def assert_fields_result_in_literal_regex(
    fields: List[TableField], pattern: str
) -> None:
    actual = get_sql_literal_from_table(Table(fields=fields))

    expected = re.compile(pattern)
    assert expected.match(
        actual
    ), f"SQL Literal:\n {actual} does not match pattern:\n {pattern}"


def test_single_field_simple_field() -> None:
    fields = [TableField(name="foo", mode=BigQueryFieldMode.NULLABLE, type="STRING")]
    assert_fields_result_in_literal_regex(fields, r"\(SELECT '(.+)' as `foo`\)")


def test_single_field_integer_field() -> None:
    fields = [TableField(name="foo", mode=BigQueryFieldMode.NULLABLE, type="INTEGER")]
    assert_fields_result_in_literal(fields, "(SELECT 1 as `foo`)")


def test_repeated_field() -> None:
    fields = [TableField(name="foo", mode=BigQueryFieldMode.REPEATED, type="STRING")]
    assert_fields_result_in_literal(fields, "(SELECT ['foo'] as `foo`)")


def test_complex_field() -> None:
    fields = [
        TableField(
            name="foo",
            type=BigQueryFieldType.STRUCT,
            fields=[TableField(name="bar", type="STRING")],
        )
    ]
    assert_fields_result_in_literal(fields, "(SELECT STRUCT('foo' as `bar`) as `foo`)")


def test_recursive_complex_field() -> None:
    fields = [
        TableField(
            name="foo",
            type=BigQueryFieldType.STRUCT,
            fields=[
                TableField(
                    name="bar",
                    type="RECORD",
                    fields=[TableField(name="baz", type="INTEGER")],
                )
            ],
        )
    ]
    assert_fields_result_in_literal(
        fields, "(SELECT STRUCT(STRUCT(1 as `baz`) as `bar`) as `foo`)"
    )


def test_repeated_complex_field() -> None:
    fields = [
        TableField(
            name="foo",
            type=BigQueryFieldType.STRUCT,
            mode=BigQueryFieldMode.REPEATED,
            fields=[TableField(name="bar", type="STRING")],
        )
    ]
    assert_fields_result_in_literal(
        fields, "(SELECT [STRUCT('foo' as `bar`)] as `foo`)"
    )


def test_replace_upstream_sql_replaces_from() -> None:
    node = SimpleNode(unique_id="A", depends_on=[]).to_node()
    original_sql = f"""
    SELECT foo
    FROM {node.to_table_ref_literal()}
    """
    table = Table(fields=[TableField(name="foo", type=BigQueryFieldType.STRING)])
    new_sql = replace_upstream_sql(original_sql, node, table)

    assert (
        new_sql
        == """
    SELECT foo
    FROM (SELECT 'foo' as `foo`)
    """
    )


def test_replace_upstream_sql_replaces_join() -> None:
    node = SimpleNode(unique_id="A", depends_on=[]).to_node()
    original_sql = f"""
    SELECT foo
    FROM `a`.`b`.`c`
    JOIN {node.to_table_ref_literal()}
    """
    table = Table(fields=[TableField(name="foo", type=BigQueryFieldType.STRING)])
    new_sql = replace_upstream_sql(original_sql, node, table)

    assert (
        new_sql
        == """
    SELECT foo
    FROM `a`.`b`.`c`
    JOIN (SELECT 'foo' as `foo`)
    """
    )


def test_replace_upstream_sql_replaces_from_newline() -> None:
    node = SimpleNode(unique_id="A", depends_on=[]).to_node()
    original_sql = f"""
    SELECT foo
    FROM
        {node.to_table_ref_literal()}
    """
    table = Table(fields=[TableField(name="foo", type=BigQueryFieldType.STRING)])
    new_sql = replace_upstream_sql(original_sql, node, table)

    assert (
        new_sql
        == """
    SELECT foo
    FROM
        (SELECT 'foo' as `foo`)
    """
    )


def test_ignores_quoted_literals() -> None:
    node = SimpleNode(unique_id="A", depends_on=[]).to_node()
    original_sql = f"""
    SELECT foo,
           '{node.to_table_ref_literal()}' AS original_table
    FROM {node.to_table_ref_literal()}
    """
    table = Table(fields=[TableField(name="foo", type=BigQueryFieldType.STRING)])
    new_sql = replace_upstream_sql(original_sql, node, table)

    assert (
        new_sql
        == f"""
    SELECT foo,
           '{node.to_table_ref_literal()}' AS original_table
    FROM (SELECT 'foo' as `foo`)
    """
    )


def test_handles_comments() -> None:
    node = SimpleNode(unique_id="A", depends_on=[]).to_node()
    original_sql = f"""
    SELECT foo
    FROM -- test
        {node.to_table_ref_literal()}
    """
    table = Table(fields=[TableField(name="foo", type=BigQueryFieldType.STRING)])
    new_sql = replace_upstream_sql(original_sql, node, table)

    assert (
        new_sql
        == """
    SELECT foo
    FROM -- test
        (SELECT 'foo' as `foo`)
    """
    )


@pytest.mark.xfail(reason="Need better regex/full SQL parsing")
def test_handles_multiple_comments() -> None:
    node = SimpleNode(unique_id="A", depends_on=[]).to_node()
    original_sql = f"""
    SELECT foo
    FROM -- test
         -- test2
        {node.to_table_ref_literal()}
    """
    table = Table(fields=[TableField(name="foo", type=BigQueryFieldType.STRING)])
    new_sql = replace_upstream_sql(original_sql, node, table)

    assert (
        new_sql
        == """
    SELECT foo
    FROM -- test
         -- test2
        (SELECT 'foo' as `foo`)
    """
    )
