from typing import List, Set

from dbt_dry_run.manifest import Manifest, NodeConfig
from dbt_dry_run.scheduler import ManifestScheduler
from dbt_dry_run.test.utils import SimpleNode


def build_manifest(nodes: List[SimpleNode]) -> Manifest:
    real_nodes = {n.unique_id: n.to_node() for n in nodes}
    return Manifest(nodes=real_nodes, macros={})


def assert_node_order(routes: List[Set[str]], manifest: Manifest) -> None:
    topological_sort = [sort_elem for sort_elem in ManifestScheduler(manifest)]
    actual_node_ids = [set([n.unique_id for n in gen]) for gen in topological_sort]
    assert actual_node_ids == routes, f"Route {actual_node_ids} was not in {routes}"


def test_linear_manifest() -> None:
    A = SimpleNode(unique_id="A", depends_on=[])
    B = SimpleNode(unique_id="B", depends_on=[A])
    C = SimpleNode(unique_id="C", depends_on=[B])
    manifest = build_manifest([A, B, C])

    assert_node_order([{"A"}, {"B"}, {"C"}], manifest)


def test_branched_manifest() -> None:
    A = SimpleNode(unique_id="A", depends_on=[])
    B = SimpleNode(unique_id="B", depends_on=[A])
    C = SimpleNode(unique_id="C", depends_on=[B])
    D = SimpleNode(unique_id="D", depends_on=[B])
    manifest = build_manifest([A, B, C, D])
    manifest2 = build_manifest([D, C, B, A])

    routes = [{"A"}, {"B"}, {"C", "D"}]
    assert_node_order(routes, manifest)
    assert_node_order(routes, manifest2)


def test_runnable_filter() -> None:
    A = SimpleNode(unique_id="A", depends_on=[])
    B = SimpleNode(unique_id="B", depends_on=[A])
    C = SimpleNode(unique_id="C", depends_on=[B], resource_type="other")
    manifest = build_manifest([A, B, C])

    assert_node_order([{"A"}, {"B"}], manifest)


def test_ephemeral_lineage() -> None:
    A = SimpleNode(unique_id="A", depends_on=[])
    B = SimpleNode(unique_id="B", depends_on=[A])
    C = SimpleNode(
        unique_id="C", depends_on=[B], table_config=NodeConfig(materialized="ephemeral")
    )
    D = SimpleNode(unique_id="D", depends_on=[C])
    manifest = build_manifest([A, B, C, D])

    assert_node_order([{"A"}, {"B"}, {"D"}], manifest)
