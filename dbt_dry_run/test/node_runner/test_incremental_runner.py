from typing import List, Optional
from unittest.mock import MagicMock, call

import pytest

from dbt_dry_run import flags
from dbt_dry_run.exception import SchemaChangeException
from dbt_dry_run.literals import enable_test_example_values
from dbt_dry_run.models import BigQueryFieldType, Table, TableField
from dbt_dry_run.models.manifest import NodeConfig, PartitionBy
from dbt_dry_run.node_runner.incremental_runner import (
    IncrementalRunner,
    append_new_columns_handler,
    get_merge_sql,
    sql_has_recursive_ctes,
    sync_all_columns_handler,
)
from dbt_dry_run.results import DryRunResult, DryRunStatus, Results
from dbt_dry_run.scheduler import ManifestScheduler
from dbt_dry_run.test.utils import SimpleNode, assert_result_has_table, get_executed_sql

enable_test_example_values(True)

A_SIMPLE_TABLE = Table(
    fields=[
        TableField(
            name="a",
            type=BigQueryFieldType.STRING,
        )
    ]
)

A_NODE = SimpleNode(
    unique_id="node1", depends_on=[], resource_type=ManifestScheduler.MODEL
).to_node()


def get_mock_sql_runner_with_all_string_columns(
    model_names: List[str], target_names: Optional[List[str]]
) -> MagicMock:
    model_schema = Table(
        fields=[
            TableField(name=name, type=BigQueryFieldType.STRING) for name in model_names
        ]
    )
    target_schema: Optional[Table] = None
    if target_names:
        target_schema = Table(
            fields=[
                TableField(name=name, type=BigQueryFieldType.STRING)
                for name in target_names
            ]
        )
    return get_mock_sql_runner_with(model_schema, target_schema)


def get_mock_sql_runner_with(
    model_schema: Table, target_schema: Optional[Table]
) -> MagicMock:
    mock_sql_runner = MagicMock()
    mock_sql_runner.query.return_value = (DryRunStatus.SUCCESS, model_schema, None)
    mock_sql_runner.get_node_schema.return_value = target_schema
    return mock_sql_runner


def test_partitioned_incremental_model_declares_dbt_max_partition_variable() -> None:
    dbt_max_partition_declaration = (
        "declare _dbt_max_partition timestamp default CURRENT_TIMESTAMP();"
    )
    mock_sql_runner = MagicMock()
    mock_sql_runner.query.return_value = (DryRunStatus.SUCCESS, A_SIMPLE_TABLE, None)

    node = SimpleNode(
        unique_id="node1",
        depends_on=[],
        resource_type=ManifestScheduler.MODEL,
        table_config=NodeConfig(
            materialized="incremental",
            partition_by=PartitionBy(
                field="loader_ingestion_time", data_type="timestamp"
            ),
        ),
        compiled_code="""
            SELECT 
                * 
            FROM `foo` 
            WHERE loader_ingestion_time >= DATE_SUB(DATE(_dbt_max_partition), INTERVAL 2 DAY)
        """,
    ).to_node()
    node.depends_on.deep_nodes = []

    results = Results()

    model_runner = IncrementalRunner(mock_sql_runner, results)
    model_runner.run(node)

    executed_sql = get_executed_sql(mock_sql_runner)
    assert executed_sql.startswith(dbt_max_partition_declaration)
    assert node.compiled_code in executed_sql


def test_incremental_model_that_does_not_exist_returns_dry_run_schema() -> None:
    mock_sql_runner = get_mock_sql_runner_with_all_string_columns(["a"], None)
    expected_table = Table(
        fields=[
            TableField(
                name="a",
                type=BigQueryFieldType.STRING,
            )
        ]
    )

    node = SimpleNode(
        unique_id="node1",
        depends_on=[],
        resource_type=ManifestScheduler.SEED,
        table_config=NodeConfig(materialized="incremental", on_schema_change="ignore"),
    ).to_node()
    node.depends_on.deep_nodes = []

    results = Results()

    model_runner = IncrementalRunner(mock_sql_runner, results)

    result = model_runner.run(node)
    mock_sql_runner.query.assert_called_with(node.compiled_code)
    assert_result_has_table(expected_table, result)


def test_incremental_model_that_exists_and_has_a_column_removed_and_readded_with_new_name() -> None:
    mock_sql_runner = get_mock_sql_runner_with_all_string_columns(
        ["a", "b"], ["a", "c"]
    )
    expected_table = Table(
        fields=[
            TableField(name="a", type=BigQueryFieldType.STRING),
            TableField(name="b", type=BigQueryFieldType.STRING),
            TableField(name="c", type=BigQueryFieldType.STRING),
        ]
    )

    node = SimpleNode(
        unique_id="node1",
        depends_on=[],
        resource_type=ManifestScheduler.SEED,
        table_config=NodeConfig(
            materialized="incremental", on_schema_change="append_new_columns"
        ),
    ).to_node()
    node.depends_on.deep_nodes = []

    results = Results()

    model_runner = IncrementalRunner(mock_sql_runner, results)

    result = model_runner.run(node)
    merge_sql = get_merge_sql(node, ["a"], node.compiled_code)
    mock_sql_runner.query.assert_has_calls([call(node.compiled_code), call(merge_sql)])
    assert_result_has_table(expected_table, result)


def test_incremental_model_that_exists_and_has_a_column_added_does_nothing() -> None:
    mock_sql_runner = get_mock_sql_runner_with_all_string_columns(["a", "b"], ["a"])
    expected_table = Table(fields=[TableField(name="a", type=BigQueryFieldType.STRING)])

    node = SimpleNode(
        unique_id="node1",
        depends_on=[],
        resource_type=ManifestScheduler.SEED,
        table_config=NodeConfig(materialized="incremental", on_schema_change="ignore"),
    ).to_node()
    node.depends_on.deep_nodes = []

    results = Results()

    model_runner = IncrementalRunner(mock_sql_runner, results)

    result = model_runner.run(node)
    merge_sql = get_merge_sql(node, ["a"], node.compiled_code)
    mock_sql_runner.query.assert_has_calls([call(node.compiled_code), call(merge_sql)])
    assert_result_has_table(expected_table, result)


def test_incremental_model_that_exists_and_has_no_common_columns() -> None:
    mock_sql_runner = get_mock_sql_runner_with_all_string_columns(
        ["a", "b"], ["c", "d"]
    )
    expected_table = Table(
        fields=[
            TableField(name="c", type=BigQueryFieldType.STRING),
            TableField(name="d", type=BigQueryFieldType.STRING),
        ]
    )

    node = SimpleNode(
        unique_id="node1",
        depends_on=[],
        resource_type=ManifestScheduler.SEED,
        table_config=NodeConfig(materialized="incremental", on_schema_change="ignore"),
    ).to_node()
    node.depends_on.deep_nodes = []

    results = Results()

    model_runner = IncrementalRunner(mock_sql_runner, results)

    result = model_runner.run(node)
    mock_sql_runner.query.assert_has_calls([call(node.compiled_code)])
    assert_result_has_table(expected_table, result)


def test_incremental_model_that_exists_and_syncs_all_columns() -> None:
    mock_sql_runner = get_mock_sql_runner_with_all_string_columns(
        ["a", "c"], ["a", "b"]
    )

    expected_table = Table(
        fields=[
            TableField(
                name="a",
                type=BigQueryFieldType.STRING,
            ),
            TableField(
                name="c",
                type=BigQueryFieldType.STRING,
            ),
        ]
    )

    node = SimpleNode(
        unique_id="node1",
        depends_on=[],
        resource_type=ManifestScheduler.SEED,
        table_config=NodeConfig(
            materialized="incremental", on_schema_change="sync_all_columns"
        ),
    ).to_node()
    node.depends_on.deep_nodes = []

    results = Results()

    model_runner = IncrementalRunner(mock_sql_runner, results)

    result = model_runner.run(node)
    merge_sql = get_merge_sql(node, ["a"], node.compiled_code)
    mock_sql_runner.query.assert_has_calls([call(node.compiled_code), call(merge_sql)])
    assert_result_has_table(expected_table, result)


def test_incremental_model_that_exists_and_fails_when_schema_changed() -> None:
    mock_sql_runner = get_mock_sql_runner_with_all_string_columns(
        ["a", "c"], ["a", "b"]
    )

    node = SimpleNode(
        unique_id="node1",
        depends_on=[],
        resource_type=ManifestScheduler.SEED,
        table_config=NodeConfig(materialized="incremental", on_schema_change="fail"),
    ).to_node()
    node.depends_on.deep_nodes = []

    results = Results()

    model_runner = IncrementalRunner(mock_sql_runner, results)

    result = model_runner.run(node)
    merge_sql = get_merge_sql(node, ["a"], node.compiled_code)
    mock_sql_runner.query.assert_has_calls([call(node.compiled_code), call(merge_sql)])
    assert result.status == DryRunStatus.FAILURE
    assert isinstance(result.exception, SchemaChangeException)


def test_incremental_model_that_exists_and_success_when_schema_not_changed() -> None:
    mock_sql_runner = get_mock_sql_runner_with_all_string_columns(
        ["a", "b"], ["a", "b"]
    )
    node = SimpleNode(
        unique_id="node1",
        depends_on=[],
        resource_type=ManifestScheduler.SEED,
        table_config=NodeConfig(materialized="incremental", on_schema_change="fail"),
    ).to_node()
    node.depends_on.deep_nodes = []

    results = Results()

    model_runner = IncrementalRunner(mock_sql_runner, results)

    result = model_runner.run(node)
    merge_sql = get_merge_sql(node, ["a", "b"], node.compiled_code)
    mock_sql_runner.query.assert_has_calls([call(node.compiled_code), call(merge_sql)])
    assert result.status == DryRunStatus.SUCCESS


def test_node_with_no_full_refresh_does_not_full_refresh_when_flag_is_false(
    default_flags: flags.Flags,
) -> None:
    flags.set_flags(flags.Flags(full_refresh=False))
    mock_sql_runner = get_mock_sql_runner_with_all_string_columns(
        ["a", "c"], ["a", "b"]
    )

    node_with_no_full_refresh_config = SimpleNode(
        unique_id="node1",
        depends_on=[],
        resource_type=ManifestScheduler.MODEL,
        table_config=NodeConfig(materialized="incremental"),
    ).to_node()
    node_with_no_full_refresh_config.depends_on.deep_nodes = []

    IncrementalRunner(mock_sql_runner, Results()).run(node_with_no_full_refresh_config)
    merge_sql = get_merge_sql(
        node_with_no_full_refresh_config,
        ["a"],
        node_with_no_full_refresh_config.compiled_code,
    )
    mock_sql_runner.query.assert_has_calls(
        [call(node_with_no_full_refresh_config.compiled_code), call(merge_sql)]
    )
    assert len(mock_sql_runner.get_node_schema.call_args_list) == 1
    mock_sql_runner.get_node_schema.assert_called_with(node_with_no_full_refresh_config)


def test_node_full_refresh_true_does_full_refresh_when_flag_is_false(
    default_flags: flags.Flags,
) -> None:
    flags.set_flags(flags.Flags(full_refresh=False))
    mock_sql_runner = get_mock_sql_runner_with_all_string_columns(
        ["a", "b"], ["a", "c"]
    )
    node_with_full_refresh_set_to_true = SimpleNode(
        unique_id="node2",
        depends_on=[],
        resource_type=ManifestScheduler.MODEL,
        table_config=NodeConfig(materialized="incremental", full_refresh=True),
    ).to_node()
    node_with_full_refresh_set_to_true.depends_on.deep_nodes = []

    IncrementalRunner(mock_sql_runner, Results()).run(
        node_with_full_refresh_set_to_true
    )
    mock_sql_runner.query.assert_has_calls(
        [call(node_with_full_refresh_set_to_true.compiled_code)]
    )
    assert (
        not mock_sql_runner.get_node_schema.called
    ), "If full refresh we do not look at the target node schema"


def test_node_full_refresh_false_does_full_refresh_when_flag_is_false(
    default_flags: flags.Flags,
) -> None:
    flags.set_flags(flags.Flags(full_refresh=False))
    mock_sql_runner = get_mock_sql_runner_with_all_string_columns(
        ["a", "c"], ["a", "b"]
    )

    node_with_full_refresh_set_to_false = SimpleNode(
        unique_id="node3",
        depends_on=[],
        resource_type=ManifestScheduler.MODEL,
        table_config=NodeConfig(materialized="incremental", full_refresh=False),
    ).to_node()
    node_with_full_refresh_set_to_false.depends_on.deep_nodes = []

    IncrementalRunner(mock_sql_runner, Results()).run(
        node_with_full_refresh_set_to_false
    )
    merge_sql = get_merge_sql(
        node_with_full_refresh_set_to_false,
        ["a"],
        node_with_full_refresh_set_to_false.compiled_code,
    )
    mock_sql_runner.query.assert_has_calls(
        [call(node_with_full_refresh_set_to_false.compiled_code), call(merge_sql)]
    )
    mock_sql_runner.get_node_schema.assert_has_calls(
        [call(node_with_full_refresh_set_to_false)]
    )


def test_node_with_no_full_refresh_does_full_refresh_when_flag_is_true(
    default_flags: flags.Flags,
) -> None:
    flags.set_flags(flags.Flags(full_refresh=True))
    mock_sql_runner = get_mock_sql_runner_with_all_string_columns(
        ["a", "c"], ["a", "b"]
    )

    node_with_no_full_refresh_config = SimpleNode(
        unique_id="node1",
        depends_on=[],
        resource_type=ManifestScheduler.MODEL,
        table_config=NodeConfig(materialized="incremental"),
    ).to_node()
    node_with_no_full_refresh_config.depends_on.deep_nodes = []

    IncrementalRunner(mock_sql_runner, Results()).run(node_with_no_full_refresh_config)
    mock_sql_runner.query.assert_called_with(
        node_with_no_full_refresh_config.compiled_code
    )
    assert not mock_sql_runner.get_node_schema.called


def test_node_with_full_refresh_does_full_refresh_when_flag_is_true(
    default_flags: flags.Flags,
) -> None:
    flags.set_flags(flags.Flags(full_refresh=True))
    mock_sql_runner = get_mock_sql_runner_with_all_string_columns(
        ["a", "c"], ["a", "b"]
    )

    node_with_full_refresh_set_to_true = SimpleNode(
        unique_id="node2",
        depends_on=[],
        resource_type=ManifestScheduler.MODEL,
        table_config=NodeConfig(materialized="incremental", full_refresh=True),
    ).to_node()
    node_with_full_refresh_set_to_true.depends_on.deep_nodes = []

    IncrementalRunner(mock_sql_runner, Results()).run(
        node_with_full_refresh_set_to_true
    )
    mock_sql_runner.query.assert_called_with(
        node_with_full_refresh_set_to_true.compiled_code
    )
    assert not mock_sql_runner.get_node_schema.called


def test_node_with_false_full_refresh_does_not_full_refresh_when_flag_is_true(
    default_flags: flags.Flags,
) -> None:
    flags.set_flags(flags.Flags(full_refresh=True))
    mock_sql_runner = get_mock_sql_runner_with_all_string_columns(
        ["a", "c"], ["a", "b"]
    )

    node_with_full_refresh_set_to_false = SimpleNode(
        unique_id="node3",
        depends_on=[],
        resource_type=ManifestScheduler.MODEL,
        table_config=NodeConfig(materialized="incremental", full_refresh=False),
    ).to_node()
    node_with_full_refresh_set_to_false.depends_on.deep_nodes = []

    IncrementalRunner(mock_sql_runner, Results()).run(
        node_with_full_refresh_set_to_false
    )
    merge_sql = get_merge_sql(
        node_with_full_refresh_set_to_false,
        ["a"],
        node_with_full_refresh_set_to_false.compiled_code,
    )
    mock_sql_runner.query.assert_has_calls(
        [call(node_with_full_refresh_set_to_false.compiled_code), call(merge_sql)]
    )
    assert len(mock_sql_runner.get_node_schema.call_args_list) == 1
    mock_sql_runner.get_node_schema.assert_called_with(
        node_with_full_refresh_set_to_false
    )


def test_model_with_sql_header_executes_header_first() -> None:
    mock_sql_runner = MagicMock()
    mock_sql_runner.query.return_value = (DryRunStatus.SUCCESS, A_SIMPLE_TABLE, None)

    pre_header_value = "DECLARE x INT64;"

    node = SimpleNode(
        unique_id="node1", depends_on=[], resource_type=ManifestScheduler.MODEL
    ).to_node()
    node.depends_on.deep_nodes = []
    node.config.sql_header = pre_header_value

    results = Results()

    model_runner = IncrementalRunner(mock_sql_runner, results)
    model_runner.run(node)

    executed_sql = get_executed_sql(mock_sql_runner)
    assert executed_sql.startswith(pre_header_value)
    assert node.compiled_code in executed_sql


def test_append_handler_preserves_existing_column_order() -> None:
    model_table = Table(
        fields=[
            TableField(name="col_1", type=BigQueryFieldType.STRING),
            TableField(name="col_2", type=BigQueryFieldType.STRING),
            TableField(name="col_3", type=BigQueryFieldType.STRING),
        ]
    )
    dry_run_result = DryRunResult(
        node=A_NODE, status=DryRunStatus.SUCCESS, table=model_table, exception=None
    )
    target_table = Table(
        fields=[
            TableField(name="col_2", type=BigQueryFieldType.STRING),
            TableField(name="col_1", type=BigQueryFieldType.STRING),
        ]
    )
    actual_result = append_new_columns_handler(dry_run_result, target_table)

    expected_table = Table(
        fields=[
            TableField(name="col_2", type=BigQueryFieldType.STRING),
            TableField(name="col_1", type=BigQueryFieldType.STRING),
            TableField(name="col_3", type=BigQueryFieldType.STRING),
        ]
    )

    assert actual_result.table == expected_table


def test_sync_handler_preserves_existing_column_order() -> None:
    model_table = Table(
        fields=[
            TableField(name="col_3", type=BigQueryFieldType.STRING),
            TableField(name="col_2", type=BigQueryFieldType.STRING),
            TableField(name="col_4", type=BigQueryFieldType.STRING),
        ]
    )
    dry_run_result = DryRunResult(
        node=A_NODE, status=DryRunStatus.SUCCESS, table=model_table, exception=None
    )
    target_table = Table(
        fields=[
            TableField(name="col_1", type=BigQueryFieldType.STRING),
            TableField(name="col_2", type=BigQueryFieldType.STRING),
            TableField(name="col_3", type=BigQueryFieldType.STRING),
        ]
    )
    actual_result = sync_all_columns_handler(dry_run_result, target_table)

    expected_table = Table(
        fields=[
            TableField(name="col_2", type=BigQueryFieldType.STRING),
            TableField(name="col_3", type=BigQueryFieldType.STRING),
            TableField(name="col_4", type=BigQueryFieldType.STRING),
        ]
    )

    assert actual_result.table == expected_table


@pytest.mark.parametrize(
    "code, has_ctes",
    [
        (
            """WITH RECURSIVE my_foo as (SELECT * FROM foo)""",
            True,
        ),
        (
            """with recursive my_foo as (SELECT * FROM foo)""",
            True,
        ),
        (
            """  with   recursive    my_foo as (SELECT * FROM foo)""",
            True,
        ),
        (
            """  with
                  RECURSIVE 
                     my_foo as (SELECT * FROM foo)""",
            True,
        ),
        (
            """  with
                     my_foo as (SELECT recursive FROM foo)""",
            False,
        ),
    ],
)
def test_sql_has_recursive_ctes(code: str, has_ctes: bool) -> None:
    assert sql_has_recursive_ctes(code) == has_ctes
