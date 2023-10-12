from unittest.mock import MagicMock

from dbt_dry_run.exception import UpstreamFailedException
from dbt_dry_run.literals import enable_test_example_values
from dbt_dry_run.models import BigQueryFieldType, Table, TableField
from dbt_dry_run.node_runner.table_runner import TableRunner
from dbt_dry_run.results import DryRunResult, DryRunStatus, Results
from dbt_dry_run.scheduler import ManifestScheduler
from dbt_dry_run.test.utils import SimpleNode, get_executed_sql

enable_test_example_values(True)

A_SIMPLE_TABLE = Table(
    fields=[
        TableField(
            name="a",
            type=BigQueryFieldType.STRING,
        )
    ]
)


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

    model_runner = TableRunner(mock_sql_runner, results)

    result = model_runner.run(node)
    mock_sql_runner.query.assert_called_with(node.compiled_code)
    assert result.status == DryRunStatus.SUCCESS
    assert result.table
    assert result.table.fields[0].name == expected_table.fields[0].name


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

    model_runner = TableRunner(mock_sql_runner, results)
    result = model_runner.run(node)
    assert result.status == DryRunStatus.FAILURE
    assert isinstance(result.exception, UpstreamFailedException)


def test_model_with_dependency_inserts_sql_literal() -> None:
    mock_sql_runner = MagicMock()
    mock_sql_runner.query.return_value = (DryRunStatus.SUCCESS, A_SIMPLE_TABLE, None)

    upstream_simple_node = SimpleNode(unique_id="upstream", depends_on=[])
    upstream_node = upstream_simple_node.to_node()
    upstream_node.depends_on.deep_nodes = []

    compiled_code = f"""SELECT * FROM {upstream_node.to_table_ref_literal()}"""

    node = SimpleNode(
        unique_id="node1",
        depends_on=[upstream_simple_node],
        resource_type=ManifestScheduler.MODEL,
        compiled_code=compiled_code,
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

    model_runner = TableRunner(mock_sql_runner, results)
    result = model_runner.run(node)

    executed_sql = get_executed_sql(mock_sql_runner)
    assert result.status == DryRunStatus.SUCCESS
    assert executed_sql == "SELECT * FROM (SELECT 'foo' as `a`)"


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

    model_runner = TableRunner(mock_sql_runner, results)
    model_runner.run(node)

    executed_sql = get_executed_sql(mock_sql_runner)
    assert executed_sql.startswith(pre_header_value)
    assert node.compiled_code in executed_sql
