from typing import Iterable, List, Optional, Set, Union

from dbt_dry_run.models.manifest import ExternalConfig, Manifest, Node, NodeConfig
from dbt_dry_run.scheduler import ManifestScheduler
from dbt_dry_run.test.utils import SimpleNode


def build_manifest(
    nodes: List[Union[Node, SimpleNode]], sources: Optional[Iterable[Node]] = None
) -> Manifest:
    real_nodes = {
        n.unique_id: n.to_node() if isinstance(n, SimpleNode) else n for n in nodes
    }
    source_map = {s.unique_id for s in sources} if sources else {}
    return Manifest(nodes=real_nodes, macros={}, sources=source_map)


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


def test_manifest_with_external_sources_includes_source_in_schedule() -> None:
    S = Node(
        unique_id="S",
        resource_type="source",
        config=NodeConfig(),
        name="s",
        database="db1",
        schema="schema1",
        original_file_path="/filepath1.yaml",
        root_path="/filepath1",
        columns={},
        external=ExternalConfig(location="location"),
        alias="s",
    )
    A = SimpleNode(unique_id="A", depends_on=[S])
    B = SimpleNode(unique_id="B", depends_on=[A])
    manifest = build_manifest([S, A, B])

    assert_node_order([{"S"}, {"A"}, {"B"}], manifest)


def test_manifest_with_normal_sources_excludes_source_in_schedule() -> None:
    S = Node(
        unique_id="S",
        resource_type="source",
        config=NodeConfig(),
        name="s",
        database="db1",
        schema="schema1",
        original_file_path="/filepath1.yaml",
        root_path="/filepath1",
        columns={},
        alias="s",
    )
    A = SimpleNode(unique_id="A", depends_on=[S])
    B = SimpleNode(unique_id="B", depends_on=[A])
    manifest = build_manifest([S, A, B])

    assert_node_order([{"A"}, {"B"}], manifest)


def test_disabled_nodes_are_not_run() -> None:
    A = SimpleNode(unique_id="A", depends_on=[])
    B = SimpleNode(
        unique_id="B", depends_on=[A], table_config=NodeConfig(enabled=False)
    )
    manifest = build_manifest([A, B])

    assert_node_order([set("A")], manifest)
