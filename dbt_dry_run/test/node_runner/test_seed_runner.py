from pathlib import Path
from typing import Set
from unittest.mock import MagicMock

from dbt_dry_run import flags
from dbt_dry_run.exception import NotCompiledException
from dbt_dry_run.flags import Flags
from dbt_dry_run.models import BigQueryFieldType
from dbt_dry_run.models.manifest import Node
from dbt_dry_run.node_runner.seed_runner import SeedRunner
from dbt_dry_run.results import DryRunResult, DryRunStatus
from dbt_dry_run.scheduler import ManifestScheduler
from dbt_dry_run.test.utils import SimpleNode


def assert_success_and_columns_equal(
    node: Node, expected_columns: Set[str]
) -> DryRunResult:
    seed_runner = SeedRunner(MagicMock(), MagicMock())
    result: DryRunResult = seed_runner.run(node)
    assert result.status == DryRunStatus.SUCCESS
    assert result.table
    fields = result.table.fields
    field_names: Set[str] = set(f.name for f in fields)
    assert field_names == expected_columns

    return result


def test_seed_runner_loads_file(tmp_path: Path) -> None:
    p = tmp_path / "seed1.csv"
    csv_content = """a,b,c
    foo,bar,baz
    foo2,bar2,baz2
    """
    p.write_text(csv_content)

    node = SimpleNode(
        unique_id="node1",
        depends_on=[],
        resource_type=ManifestScheduler.SEED,
        original_file_path=p.as_posix(),
    ).to_node()
    expected_columns = set(csv_content.splitlines()[0].split(","))
    assert_success_and_columns_equal(node, expected_columns)


def test_seed_runner_infers_dates(tmp_path: Path) -> None:
    p = tmp_path / "seed1.csv"
    csv_content = """a,b,c
    foo,bar,2021-01-01
    foo2,bar2,2021-01-01
    """
    p.write_text(csv_content)

    node = SimpleNode(
        unique_id="node1",
        depends_on=[],
        resource_type=ManifestScheduler.SEED,
        original_file_path=p.as_posix(),
    ).to_node()
    expected_columns = set(csv_content.splitlines()[0].split(","))
    result = assert_success_and_columns_equal(node, expected_columns)

    assert result.table
    assert result.table.fields[2].type_ == BigQueryFieldType.DATE


def test_validate_node_returns_none_if_node_is_not_compiled() -> None:
    mock_sql_runner = MagicMock()
    results = MagicMock()

    node = SimpleNode(
        unique_id="node1", depends_on=[], resource_type=ManifestScheduler.MODEL
    ).to_node()
    node.compiled = False

    model_runner = SeedRunner(mock_sql_runner, results)

    validation_result = model_runner.validate_node(node)
    assert validation_result is None
