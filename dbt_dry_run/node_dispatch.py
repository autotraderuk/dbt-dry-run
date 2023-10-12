from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Type

from dbt_dry_run.models.manifest import Node
from dbt_dry_run.node_runner import NodeRunner
from dbt_dry_run.node_runner.incremental_runner import IncrementalRunner
from dbt_dry_run.node_runner.node_test_runner import NodeTestRunner
from dbt_dry_run.node_runner.seed_runner import SeedRunner
from dbt_dry_run.node_runner.snapshot_runner import SnapshotRunner
from dbt_dry_run.node_runner.source_runner import SourceRunner
from dbt_dry_run.node_runner.table_runner import TableRunner
from dbt_dry_run.node_runner.view_runner import ViewRunner
from dbt_dry_run.results import DryRunResult


@dataclass(frozen=True, eq=True)
class RunnerKey:
    resource_type: str
    materialized: Optional[str] = None


RUNNERS: Dict[RunnerKey, Type[NodeRunner]] = {
    RunnerKey("model", "incremental"): IncrementalRunner,
    RunnerKey("model", "table"): TableRunner,
    RunnerKey("model", "view"): ViewRunner,
    RunnerKey("test", "test"): NodeTestRunner,
    RunnerKey("snapshot", "snapshot"): SnapshotRunner,
    RunnerKey("seed", "seed"): SeedRunner,
    RunnerKey("source"): SourceRunner,
}


def _get_node_runner_key(node: Node) -> RunnerKey:
    return RunnerKey(node.resource_type, node.config.materialized)


def dispatch_node(node: Node, runners: Dict[RunnerKey, NodeRunner]) -> DryRunResult:
    _runner_key = _get_node_runner_key(node)
    try:
        runner = runners[_runner_key]
    except KeyError:
        raise ValueError(f"Unknown node '{_runner_key}'")
    validation_result = runner.validate_node(node)
    if validation_result:
        return validation_result
    return runner.run(node)
