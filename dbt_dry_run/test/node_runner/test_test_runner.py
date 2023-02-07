from unittest.mock import MagicMock

from dbt_dry_run import flags
from dbt_dry_run.exception import NotCompiledException
from dbt_dry_run.literals import enable_test_example_values
from dbt_dry_run.models import BigQueryFieldType, Table, TableField
from dbt_dry_run.node_runner.node_test_runner import NodeTestRunner
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


def test_test_runs_sql() -> None:
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

    test_node = SimpleNode(
        unique_id="test1", depends_on=[], resource_type=ManifestScheduler.TEST
    ).to_node()
    test_node.depends_on.deep_nodes = []

    results = Results()

    model_runner = NodeTestRunner(mock_sql_runner, results)

    result = model_runner.run(test_node)
    mock_sql_runner.query.assert_called_with(test_node.compiled_code)
    assert result.status == DryRunStatus.SUCCESS
    assert result.table
    assert result.table.fields[0].name == expected_table.fields[0].name


def test_validate_node_fails_if_skip_not_compiled_is_false(
    default_flags: flags.Flags,
) -> None:
    flags.set_flags(flags.Flags(skip_not_compiled=False))
    mock_sql_runner = MagicMock()
    results = MagicMock()

    node = SimpleNode(
        unique_id="node1", depends_on=[], resource_type=ManifestScheduler.MODEL
    ).to_node()
    node.compiled = False

    model_runner = NodeTestRunner(mock_sql_runner, results)

    validation_result = model_runner.validate_node(node)
    assert validation_result
    assert validation_result.status == DryRunStatus.FAILURE
    assert isinstance(validation_result.exception, NotCompiledException)


def test_validate_node_skips_if_skip_not_compiled_is_true(
    default_flags: flags.Flags,
) -> None:
    flags.set_flags(flags.Flags(skip_not_compiled=True))
    mock_sql_runner = MagicMock()
    results = MagicMock()

    node = SimpleNode(
        unique_id="node1", depends_on=[], resource_type=ManifestScheduler.MODEL
    ).to_node()
    node.compiled = False

    model_runner = NodeTestRunner(mock_sql_runner, results)

    validation_result = model_runner.validate_node(node)
    assert validation_result
    assert validation_result.status == DryRunStatus.SKIPPED
    assert validation_result.exception is None
