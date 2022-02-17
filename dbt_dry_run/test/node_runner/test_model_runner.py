from unittest.mock import MagicMock

from dbt_dry_run.exception import SchemaChangeException, UpstreamFailedException
from dbt_dry_run.literals import enable_test_example_values
from dbt_dry_run.manifest import NodeConfig
from dbt_dry_run.models import (
    BigQueryFieldType,
    DryRunResult,
    DryRunStatus,
    Table,
    TableField,
)
from dbt_dry_run.node_runner.model_runner import ModelRunner
from dbt_dry_run.results import Results
from dbt_dry_run.scheduler import ManifestScheduler
from dbt_dry_run.test.utils import SimpleNode

enable_test_example_values(True)

VIEW_CREATION_SQL = "CREATE OR REPLACE VIEW"

A_SIMPLE_TABLE = Table(
    fields=[
        TableField(
            name="a",
            type=BigQueryFieldType.STRING,
        )
    ]
)


def get_executed_sql(mock: MagicMock) -> str:
    call_args = mock.query.call_args_list
    assert len(call_args) == 1
    executed_sql = call_args[0].args[0]
    return executed_sql


def test_model_with_no_dependencies_runs_sql() -> None:
    mock_sql_runner = MagicMock()
    expected_table = Table(
        fields=[
            TableField(
                name="a",
                type=BigQueryFieldType.STRING,
            )
        ]
    )
    mock_sql_runner.query.return_value = (DryRunStatus.SUCCESS, expected_table, None)

    node = SimpleNode(
        unique_id="node1", depends_on=[], resource_type=ManifestScheduler.SEED
    ).to_node()
    node.depends_on.deep_nodes = []

    results = Results()

    model_runner = ModelRunner(mock_sql_runner, results)

    result = model_runner.run(node)
    mock_sql_runner.query.assert_called_with(node.compiled_sql)
    assert result.status == DryRunStatus.SUCCESS
    assert result.table
    assert result.table.fields[0].name == expected_table.fields[0].name


def test_model_as_view_runs_create_view() -> None:
    mock_sql_runner = MagicMock()
    mock_sql_runner.query.return_value = (DryRunStatus.SUCCESS, A_SIMPLE_TABLE, None)

    node = SimpleNode(
        unique_id="node1", depends_on=[], resource_type=ManifestScheduler.MODEL
    ).to_node()
    node.depends_on.deep_nodes = []
    node.config.materialized = "view"

    results = Results()

    model_runner = ModelRunner(mock_sql_runner, results)
    model_runner.run(node)

    executed_sql = get_executed_sql(mock_sql_runner)
    assert executed_sql.startswith(VIEW_CREATION_SQL)
    assert node.compiled_sql in executed_sql


def test_model_with_failed_dependency_raises_upstream_failed_exception() -> None:
    mock_sql_runner = MagicMock()
    mock_sql_runner.query.return_value = (DryRunStatus.SUCCESS, A_SIMPLE_TABLE, None)

    upstream_simple_node = SimpleNode(unique_id="upstream", depends_on=[])
    upstream_node = upstream_simple_node.to_node()
    upstream_node.depends_on.deep_nodes = []

    node = SimpleNode(
        unique_id="node1",
        depends_on=[upstream_simple_node],
        resource_type=ManifestScheduler.MODEL,
    ).to_node()
    node.depends_on.deep_nodes = ["upstream"]

    results = Results()
    results.add_result(
        "upstream",
        DryRunResult(
            node=upstream_node,
            status=DryRunStatus.FAILURE,
            table=None,
            exception=Exception("BOOM"),
        ),
    )

    model_runner = ModelRunner(mock_sql_runner, results)
    result = model_runner.run(node)
    assert result.status == DryRunStatus.FAILURE
    assert isinstance(result.exception, UpstreamFailedException)


def test_model_with_dependency_inserts_sql_literal() -> None:
    mock_sql_runner = MagicMock()
    mock_sql_runner.query.return_value = (DryRunStatus.SUCCESS, A_SIMPLE_TABLE, None)

    upstream_simple_node = SimpleNode(unique_id="upstream", depends_on=[])
    upstream_node = upstream_simple_node.to_node()
    upstream_node.depends_on.deep_nodes = []

    compiled_sql = f"""SELECT * FROM {upstream_node.to_table_ref_literal()}"""

    node = SimpleNode(
        unique_id="node1",
        depends_on=[upstream_simple_node],
        resource_type=ManifestScheduler.MODEL,
        compiled_sql=compiled_sql,
    ).to_node()
    node.depends_on.deep_nodes = ["upstream"]

    results = Results()
    results.add_result(
        "upstream",
        DryRunResult(
            node=upstream_node,
            status=DryRunStatus.SUCCESS,
            table=A_SIMPLE_TABLE,
            exception=Exception("BOOM"),
        ),
    )

    model_runner = ModelRunner(mock_sql_runner, results)
    result = model_runner.run(node)

    executed_sql = get_executed_sql(mock_sql_runner)
    assert result.status == DryRunStatus.SUCCESS
    assert executed_sql == "SELECT * FROM (SELECT 'foo' as `a`)"


def test_incremental_model_that_does_not_exist_returns_dry_run_schema() -> None:
    mock_sql_runner = MagicMock()
    predicted_table = Table(
        fields=[
            TableField(
                name="a",
                type=BigQueryFieldType.STRING,
            )
        ]
    )
    mock_sql_runner.query.return_value = (DryRunStatus.SUCCESS, predicted_table, None)
    mock_sql_runner.get_node_schema.return_value = None

    node = SimpleNode(
        unique_id="node1",
        depends_on=[],
        resource_type=ManifestScheduler.SEED,
        table_config=NodeConfig(materialized="incremental", on_schema_change="ignore"),
    ).to_node()
    node.depends_on.deep_nodes = []

    results = Results()

    model_runner = ModelRunner(mock_sql_runner, results)

    result = model_runner.run(node)
    mock_sql_runner.query.assert_called_with(node.compiled_sql)
    assert_result_has_table(predicted_table, result)


def test_incremental_model_that_exists_and_has_a_column_removed_and_readded_with_new_name() -> None:
    mock_sql_runner = MagicMock()
    target_table = Table(
        fields=[
            TableField(name="a", type=BigQueryFieldType.STRING),
            TableField(name="b", type=BigQueryFieldType.STRING),
        ]
    )
    predicted_table = Table(
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
    expected_table = Table(
        fields=[
            TableField(name="a", type=BigQueryFieldType.STRING),
            TableField(name="b", type=BigQueryFieldType.STRING),
            TableField(name="c", type=BigQueryFieldType.STRING),
        ]
    )

    mock_sql_runner.query.return_value = (DryRunStatus.SUCCESS, predicted_table, None)
    mock_sql_runner.get_node_schema.return_value = target_table

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

    model_runner = ModelRunner(mock_sql_runner, results)

    result = model_runner.run(node)
    mock_sql_runner.query.assert_called_with(node.compiled_sql)
    assert_result_has_table(expected_table, result)


def test_incremental_model_that_exists_and_has_a_column_added_does_nothing() -> None:
    mock_sql_runner = MagicMock()
    target_table = Table(fields=[TableField(name="a", type=BigQueryFieldType.STRING)])
    predicted_table = Table(
        fields=[
            TableField(
                name="a",
                type=BigQueryFieldType.STRING,
            ),
            TableField(
                name="b",
                type=BigQueryFieldType.STRING,
            ),
        ]
    )
    expected_table = target_table

    mock_sql_runner.query.return_value = (DryRunStatus.SUCCESS, predicted_table, None)
    mock_sql_runner.get_node_schema.return_value = target_table

    node = SimpleNode(
        unique_id="node1",
        depends_on=[],
        resource_type=ManifestScheduler.SEED,
        table_config=NodeConfig(materialized="incremental", on_schema_change="ignore"),
    ).to_node()
    node.depends_on.deep_nodes = []

    results = Results()

    model_runner = ModelRunner(mock_sql_runner, results)

    result = model_runner.run(node)
    mock_sql_runner.query.assert_called_with(node.compiled_sql)
    assert_result_has_table(expected_table, result)


def test_incremental_model_that_exists_and_syncs_all_columns() -> None:
    mock_sql_runner = MagicMock()
    target_table = Table(
        fields=[
            TableField(name="a", type=BigQueryFieldType.STRING),
            TableField(name="b", type=BigQueryFieldType.STRING),
        ]
    )
    predicted_table = Table(
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

    expected_table = predicted_table

    mock_sql_runner.query.return_value = (DryRunStatus.SUCCESS, predicted_table, None)
    mock_sql_runner.get_node_schema.return_value = target_table

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

    model_runner = ModelRunner(mock_sql_runner, results)

    result = model_runner.run(node)
    mock_sql_runner.query.assert_called_with(node.compiled_sql)
    assert_result_has_table(expected_table, result)


def test_incremental_model_that_exists_and_fails_when_schema_changed() -> None:
    mock_sql_runner = MagicMock()
    target_table = Table(
        fields=[
            TableField(name="a", type=BigQueryFieldType.STRING),
            TableField(name="b", type=BigQueryFieldType.STRING),
        ]
    )
    predicted_table = Table(
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

    mock_sql_runner.query.return_value = (DryRunStatus.SUCCESS, predicted_table, None)
    mock_sql_runner.get_node_schema.return_value = target_table

    node = SimpleNode(
        unique_id="node1",
        depends_on=[],
        resource_type=ManifestScheduler.SEED,
        table_config=NodeConfig(materialized="incremental", on_schema_change="fail"),
    ).to_node()
    node.depends_on.deep_nodes = []

    results = Results()

    model_runner = ModelRunner(mock_sql_runner, results)

    result = model_runner.run(node)
    mock_sql_runner.query.assert_called_with(node.compiled_sql)
    assert result.status == DryRunStatus.FAILURE
    assert isinstance(result.exception, SchemaChangeException)


def test_incremental_model_that_exists_and_success_when_schema_not_changed() -> None:
    mock_sql_runner = MagicMock()
    target_table = Table(
        fields=[
            TableField(name="a", type=BigQueryFieldType.STRING),
            TableField(name="b", type=BigQueryFieldType.STRING),
        ]
    )
    predicted_table = Table(
        fields=[
            TableField(
                name="a",
                type=BigQueryFieldType.STRING,
            ),
            TableField(
                name="b",
                type=BigQueryFieldType.STRING,
            ),
        ]
    )

    mock_sql_runner.query.return_value = (DryRunStatus.SUCCESS, predicted_table, None)
    mock_sql_runner.get_node_schema.return_value = target_table

    node = SimpleNode(
        unique_id="node1",
        depends_on=[],
        resource_type=ManifestScheduler.SEED,
        table_config=NodeConfig(materialized="incremental", on_schema_change="fail"),
    ).to_node()
    node.depends_on.deep_nodes = []

    results = Results()

    model_runner = ModelRunner(mock_sql_runner, results)

    result = model_runner.run(node)
    mock_sql_runner.query.assert_called_with(node.compiled_sql)
    assert result.status == DryRunStatus.SUCCESS


def assert_result_has_table(expected: Table, actual: DryRunResult) -> None:
    assert actual.status == DryRunStatus.SUCCESS
    assert actual.table

    actual_field_names = set([field.name for field in actual.table.fields])
    expected_field_names = set([field.name for field in expected.fields])

    assert (
        actual_field_names == expected_field_names
    ), f"Actual field names: {actual_field_names} did not equal expected: {expected_field_names}"


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

    model_runner = ModelRunner(mock_sql_runner, results)
    model_runner.run(node)

    executed_sql = get_executed_sql(mock_sql_runner)
    assert executed_sql.startswith(pre_header_value)
    assert node.compiled_sql in executed_sql
