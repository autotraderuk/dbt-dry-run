from pathlib import Path
from typing import Optional, Set
from unittest.mock import MagicMock

from dbt_dry_run import flags
from dbt_dry_run.exception import NotCompiledException, UnknownSchemaException
from dbt_dry_run.flags import Flags
from dbt_dry_run.models import BigQueryFieldType
from dbt_dry_run.models.manifest import Node
from dbt_dry_run.node_runner.seed_runner import SeedRunner
from dbt_dry_run.results import DryRunResult, DryRunStatus
from dbt_dry_run.scheduler import ManifestScheduler
from dbt_dry_run.test.utils import SimpleNode


def get_result(node: Node, column_type: Optional[str] = "string") -> DryRunResult:
    mock_sql_runner = MagicMock()
    mock_sql_runner.convert_agate_type.return_value = column_type
    seed_runner = SeedRunner(mock_sql_runner, MagicMock())
    return seed_runner.run(node)


def assert_success_and_columns_equal(
    node: Node, expected_columns: Set[str], column_type: Optional[str] = "string"
) -> DryRunResult:
    result = get_result(node, column_type)
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


def test_seed_runner_fails_if_type_returns_none(tmp_path: Path) -> None:
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
    result = get_result(node, None)
    assert result.status == DryRunStatus.FAILURE
    assert type(result.exception) == UnknownSchemaException


def test_seed_runner_uses_column_overrides(tmp_path: Path) -> None:
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
    node.config.column_types = {"a": "NUMERIC"}
    result = get_result(node, "STRING")

    expected_fields = {"a": "NUMERIC", "b": "STRING", "c": "STRING"}
    assert result.table, "Expected result to have a table"
    mapped_fields = {field.name: field.type_ for field in result.table.fields}
    assert mapped_fields == expected_fields


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
