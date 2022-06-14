from unittest.mock import MagicMock

from dbt_dry_run.literals import enable_test_example_values
from dbt_dry_run.models import BigQueryFieldType, Table, TableField
from dbt_dry_run.models.manifest import NodeConfig
from dbt_dry_run.node_runner.snapshot_runner import SnapshotRunner
from dbt_dry_run.results import DryRunStatus, Results
from dbt_dry_run.scheduler import ManifestScheduler
from dbt_dry_run.test.utils import SimpleNode

enable_test_example_values(True)

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


def test_snapshot_with_check_all_strategy_runs_sql_with_id() -> None:
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
        unique_id="node1",
        depends_on=[],
        resource_type=ManifestScheduler.SNAPSHOT,
        table_config=NodeConfig(
            unique_key="a", strategy="check", check_cols="all", materialized="snapshot"
        ),
    ).to_node()
    node.depends_on.deep_nodes = []

    results = Results()

    model_runner = SnapshotRunner(mock_sql_runner, results)

    result = model_runner.run(node)
    mock_sql_runner.query.assert_called_with(node.compiled_sql)
    assert (
        result.status == DryRunStatus.SUCCESS
    ), f"Failed with error: {result.exception}"
    assert result.table
    assert result.table.fields[0].name == expected_table.fields[0].name


def test_snapshot_with_check_all_strategy_fails_without_id() -> None:
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
        unique_id="node1",
        depends_on=[],
        resource_type=ManifestScheduler.SNAPSHOT,
        table_config=NodeConfig(
            unique_key="a_missing_column",
            strategy="check",
            check_cols="all",
            materialized="snapshot",
        ),
    ).to_node()
    node.depends_on.deep_nodes = []

    results = Results()

    model_runner = SnapshotRunner(mock_sql_runner, results)

    result = model_runner.run(node)
    mock_sql_runner.query.assert_called_with(node.compiled_sql)
    assert result.status == DryRunStatus.FAILURE


def test_snapshot_with_check_all_strategy_runs_sql_with_matching_columns() -> None:
    mock_sql_runner = MagicMock()
    expected_table = Table(
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
    mock_sql_runner.query.return_value = (DryRunStatus.SUCCESS, expected_table, None)

    node = SimpleNode(
        unique_id="node1",
        depends_on=[],
        resource_type=ManifestScheduler.SNAPSHOT,
        table_config=NodeConfig(
            unique_key="a",
            strategy="check",
            check_cols=["a", "b"],
            materialized="snapshot",
        ),
    ).to_node()
    node.depends_on.deep_nodes = []

    results = Results()

    model_runner = SnapshotRunner(mock_sql_runner, results)

    result = model_runner.run(node)
    mock_sql_runner.query.assert_called_with(node.compiled_sql)
    assert (
        result.status == DryRunStatus.SUCCESS
    ), f"Failed with error: {result.exception}"
    assert result.table
    assert result.table.fields[0].name == expected_table.fields[0].name


def test_snapshot_with_check_cols_strategy_fails_with_missing_column() -> None:
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
        unique_id="node1",
        depends_on=[],
        resource_type=ManifestScheduler.SNAPSHOT,
        table_config=NodeConfig(
            unique_key="a",
            strategy="check",
            check_cols=["a", "b"],
            materialized="snapshot",
        ),
    ).to_node()
    node.depends_on.deep_nodes = []

    results = Results()

    model_runner = SnapshotRunner(mock_sql_runner, results)

    result = model_runner.run(node)
    mock_sql_runner.query.assert_called_with(node.compiled_sql)
    assert result.status == DryRunStatus.FAILURE


def test_snapshot_with_timestamp_strategy_with_updated_at_column() -> None:
    mock_sql_runner = MagicMock()
    expected_table = Table(
        fields=[
            TableField(
                name="a",
                type=BigQueryFieldType.STRING,
            ),
            TableField(name="last_updated_col", type=BigQueryFieldType.TIMESTAMP),
        ]
    )
    mock_sql_runner.query.return_value = (DryRunStatus.SUCCESS, expected_table, None)

    node = SimpleNode(
        unique_id="node1",
        depends_on=[],
        resource_type=ManifestScheduler.SNAPSHOT,
        table_config=NodeConfig(
            unique_key="a",
            strategy="timestamp",
            materialized="snapshot",
            updated_at="last_updated_col",
        ),
    ).to_node()
    node.depends_on.deep_nodes = []

    results = Results()

    model_runner = SnapshotRunner(mock_sql_runner, results)

    result = model_runner.run(node)
    mock_sql_runner.query.assert_called_with(node.compiled_sql)
    assert result.status == DryRunStatus.SUCCESS


def test_snapshot_with_timestamp_strategy_with_missing_updated_at_column() -> None:
    mock_sql_runner = MagicMock()
    expected_table = Table(
        fields=[
            TableField(
                name="a",
                type=BigQueryFieldType.STRING,
            ),
            TableField(name="last_updated_col", type=BigQueryFieldType.TIMESTAMP),
        ]
    )
    mock_sql_runner.query.return_value = (DryRunStatus.SUCCESS, expected_table, None)

    node = SimpleNode(
        unique_id="node1",
        depends_on=[],
        resource_type=ManifestScheduler.SNAPSHOT,
        table_config=NodeConfig(
            unique_key="a",
            strategy="timestamp",
            materialized="snapshot",
            updated_at="wrong_last_updated_col",
        ),
    ).to_node()
    node.depends_on.deep_nodes = []

    results = Results()

    model_runner = SnapshotRunner(mock_sql_runner, results)

    result = model_runner.run(node)
    mock_sql_runner.query.assert_called_with(node.compiled_sql)
    assert result.status == DryRunStatus.FAILURE
