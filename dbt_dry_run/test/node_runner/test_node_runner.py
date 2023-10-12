from unittest.mock import MagicMock

from dbt_dry_run import flags
from dbt_dry_run.exception import NotCompiledException
from dbt_dry_run.flags import Flags
from dbt_dry_run.node_runner.incremental_runner import IncrementalRunner
from dbt_dry_run.results import DryRunStatus
from dbt_dry_run.scheduler import ManifestScheduler
from dbt_dry_run.test.utils import SimpleNode


def test_validate_node_fails_if_skip_not_compiled_is_false(
    default_flags: Flags,
) -> None:
    flags.set_flags(flags.Flags(skip_not_compiled=False))
    mock_sql_runner = MagicMock()
    results = MagicMock()

    node = SimpleNode(
        unique_id="node1", depends_on=[], resource_type=ManifestScheduler.MODEL
    ).to_node()
    node.compiled = False

    model_runner = IncrementalRunner(mock_sql_runner, results)

    validation_result = model_runner.validate_node(node)
    assert validation_result
    assert validation_result.status == DryRunStatus.FAILURE
    assert isinstance(validation_result.exception, NotCompiledException)


def test_validate_node_skips_if_skip_not_compiled_is_true(default_flags: Flags) -> None:
    flags.set_flags(flags.Flags(skip_not_compiled=True))
    mock_sql_runner = MagicMock()
    results = MagicMock()

    node = SimpleNode(
        unique_id="node1", depends_on=[], resource_type=ManifestScheduler.MODEL
    ).to_node()
    node.compiled = False

    model_runner = IncrementalRunner(mock_sql_runner, results)

    validation_result = model_runner.validate_node(node)
    assert validation_result
    assert validation_result.status == DryRunStatus.SKIPPED
    assert validation_result.exception is None
