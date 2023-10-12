from unittest.mock import MagicMock

from dbt_dry_run import flags
from dbt_dry_run.exception import SchemaChangeException
from dbt_dry_run.literals import enable_test_example_values
from dbt_dry_run.models import BigQueryFieldType, Table, TableField
from dbt_dry_run.models.manifest import NodeConfig, PartitionBy
from dbt_dry_run.node_runner.incremental_runner import IncrementalRunner
from dbt_dry_run.results import DryRunStatus, Results
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

    model_runner = IncrementalRunner(mock_sql_runner, results)

    result = model_runner.run(node)
    mock_sql_runner.query.assert_called_with(node.compiled_code)
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

    model_runner = IncrementalRunner(mock_sql_runner, results)

    result = model_runner.run(node)
    mock_sql_runner.query.assert_called_with(node.compiled_code)
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

    model_runner = IncrementalRunner(mock_sql_runner, results)

    result = model_runner.run(node)
    mock_sql_runner.query.assert_called_with(node.compiled_code)
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

    model_runner = IncrementalRunner(mock_sql_runner, results)

    result = model_runner.run(node)
    mock_sql_runner.query.assert_called_with(node.compiled_code)
    assert_result_has_table(expected_table, result)


def test_usage_of_predicted_table_and_target_table_when_full_refresh_flag_is_false(
    default_flags: flags.Flags,
) -> None:
    flags.set_flags(flags.Flags(full_refresh=False))
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

    node_with_no_full_refresh_config = SimpleNode(
        unique_id="node1",
        depends_on=[],
        resource_type=ManifestScheduler.MODEL,
        table_config=NodeConfig(materialized="incremental"),
    ).to_node()
    node_with_no_full_refresh_config.depends_on.deep_nodes = []
    node_with_full_refresh_set_to_true = SimpleNode(
        unique_id="node2",
        depends_on=[],
        resource_type=ManifestScheduler.MODEL,
        table_config=NodeConfig(materialized="incremental", full_refresh=True),
    ).to_node()
    node_with_full_refresh_set_to_true.depends_on.deep_nodes = []
    node_with_full_refresh_set_to_false = SimpleNode(
        unique_id="node3",
        depends_on=[],
        resource_type=ManifestScheduler.MODEL,
        table_config=NodeConfig(materialized="incremental", full_refresh=False),
    ).to_node()
    node_with_full_refresh_set_to_false.depends_on.deep_nodes = []

    IncrementalRunner(mock_sql_runner, Results()).run(node_with_no_full_refresh_config)
    mock_sql_runner.query.assert_called_with(
        node_with_no_full_refresh_config.compiled_code
    )
    assert len(mock_sql_runner.get_node_schema.call_args_list) == 1
    mock_sql_runner.get_node_schema.assert_called_with(node_with_no_full_refresh_config)

    IncrementalRunner(mock_sql_runner, Results()).run(
        node_with_full_refresh_set_to_true
    )
    mock_sql_runner.query.assert_called_with(
        node_with_full_refresh_set_to_true.compiled_code
    )
    assert len(mock_sql_runner.get_node_schema.call_args_list) == 1

    IncrementalRunner(mock_sql_runner, Results()).run(
        node_with_full_refresh_set_to_false
    )
    mock_sql_runner.query.assert_called_with(
        node_with_full_refresh_set_to_false.compiled_code
    )
    assert len(mock_sql_runner.get_node_schema.call_args_list) == 2
    mock_sql_runner.get_node_schema.assert_called_with(
        node_with_full_refresh_set_to_false
    )


def test_usage_of_predicted_table_and_target_table_when_full_refresh_flag_is_true(
    default_flags: flags.Flags,
) -> None:
    flags.set_flags(flags.Flags(full_refresh=True))
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

    node_with_no_full_refresh_config = SimpleNode(
        unique_id="node1",
        depends_on=[],
        resource_type=ManifestScheduler.MODEL,
        table_config=NodeConfig(materialized="incremental"),
    ).to_node()
    node_with_no_full_refresh_config.depends_on.deep_nodes = []
    node_with_full_refresh_set_to_true = SimpleNode(
        unique_id="node2",
        depends_on=[],
        resource_type=ManifestScheduler.MODEL,
        table_config=NodeConfig(materialized="incremental", full_refresh=True),
    ).to_node()
    node_with_full_refresh_set_to_true.depends_on.deep_nodes = []
    node_with_full_refresh_set_to_false = SimpleNode(
        unique_id="node3",
        depends_on=[],
        resource_type=ManifestScheduler.MODEL,
        table_config=NodeConfig(materialized="incremental", full_refresh=False),
    ).to_node()
    node_with_full_refresh_set_to_false.depends_on.deep_nodes = []

    IncrementalRunner(mock_sql_runner, Results()).run(node_with_no_full_refresh_config)
    mock_sql_runner.query.assert_called_with(
        node_with_no_full_refresh_config.compiled_code
    )
    assert len(mock_sql_runner.get_node_schema.call_args_list) == 0

    IncrementalRunner(mock_sql_runner, Results()).run(
        node_with_full_refresh_set_to_true
    )
    mock_sql_runner.query.assert_called_with(
        node_with_full_refresh_set_to_true.compiled_code
    )
    assert len(mock_sql_runner.get_node_schema.call_args_list) == 0

    IncrementalRunner(mock_sql_runner, Results()).run(
        node_with_full_refresh_set_to_false
    )
    mock_sql_runner.query.assert_called_with(
        node_with_full_refresh_set_to_false.compiled_code
    )
    assert len(mock_sql_runner.get_node_schema.call_args_list) == 1
    mock_sql_runner.get_node_schema.assert_called_with(
        node_with_full_refresh_set_to_false
    )


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

    model_runner = IncrementalRunner(mock_sql_runner, results)

    result = model_runner.run(node)
    mock_sql_runner.query.assert_called_with(node.compiled_code)
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

    model_runner = IncrementalRunner(mock_sql_runner, results)

    result = model_runner.run(node)
    mock_sql_runner.query.assert_called_with(node.compiled_code)
    assert result.status == DryRunStatus.SUCCESS


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
