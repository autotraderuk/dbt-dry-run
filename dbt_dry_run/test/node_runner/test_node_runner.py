from typing import cast

from dbt_dry_run.models import Node
from dbt_dry_run.node_runner import NodeRunner, get_runner_map
from dbt_dry_run.results import DryRunResult


class SingleNodeRunner(NodeRunner):
    resource_type = ("single",)

    def run(self, node: Node) -> DryRunResult:
        return cast(DryRunResult, None)


class MultiNodeRunner(NodeRunner):
    resource_type = ("first", "second")

    def run(self, node: Node) -> DryRunResult:
        return cast(DryRunResult, None)


def test_get_runner_map_single_resource_type() -> None:
    runner_map = get_runner_map([SingleNodeRunner])
    assert {SingleNodeRunner.resource_type[0]: SingleNodeRunner} == runner_map


def test_get_runner_map_multi_resource_type() -> None:
    runner_map = get_runner_map([SingleNodeRunner, MultiNodeRunner])
    assert {
        SingleNodeRunner.resource_type[0]: SingleNodeRunner,
        MultiNodeRunner.resource_type[0]: MultiNodeRunner,
        MultiNodeRunner.resource_type[1]: MultiNodeRunner,
    } == runner_map
