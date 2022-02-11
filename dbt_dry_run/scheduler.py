from itertools import chain
from typing import Dict, Iterator, List, Optional, Set

from networkx import DiGraph, from_dict_of_lists, topological_generations

from dbt_dry_run.manifest import Manifest, Node


class ManifestScheduler:
    MODEL = "model"
    SEED = "seed"
    RUNNABLE_RESOURCE_TYPE = (MODEL, SEED)
    RUNNABLE_MATERIAL = ("view", "table", "incremental", "seed")

    def __init__(self, manifest: Manifest, model: Optional[str] = None):
        self._manifest = manifest
        self._model_filter = model
        self._status: Dict[str, bool] = {
            node_key: False for node_key in self._manifest.nodes.keys()
        }

    def _filter_manifest(self) -> Set[str]:
        if self._model_filter is None:
            return set(self._manifest.nodes.keys())
        try:
            leaf_node = self._manifest.nodes[self._model_filter]
        except KeyError:
            raise KeyError(f"Model {self._model_filter} does not exist in manifest")
        upstream_node_keys = leaf_node.depends_on.nodes
        filtered_nodes = [self._model_filter, *upstream_node_keys]
        while upstream_node_keys:
            upstream_nodes = list(
                filter(
                    lambda val: val is not None,
                    [self._manifest.nodes.get(k) for k in upstream_node_keys],
                )
            )
            upstream_node_keys = list(
                filter(
                    lambda n: n in self._manifest.nodes.keys(),
                    chain.from_iterable(
                        [n.depends_on.nodes for n in upstream_nodes if n]
                    ),
                )
            )
            filtered_nodes.extend(upstream_node_keys)
        return set(filtered_nodes)

    def _get_runnable_keys(self) -> Set[str]:
        remaining_nodes = set(
            filter(self._node_key_is_runnable, self._manifest.nodes.keys())
        )

        if self._model_filter:
            remaining_nodes = remaining_nodes.intersection(self._filter_manifest())
            if self._model_filter not in remaining_nodes:
                model_filter_config = self._manifest.nodes[self._model_filter].config
                model_message = (
                    f"Model {self._model_filter} is not runnable: {model_filter_config}"
                )
                raise KeyError(model_message)
        return remaining_nodes

    def __iter__(self) -> Iterator[List[Node]]:
        generation: List[str]
        for gen_id, generation in enumerate(self._calculate_depths()):
            nodes = [
                self._manifest.nodes[k] for k in generation if k in self._manifest.nodes
            ]
            yield nodes  # list(filter(self._node_is_runnable, nodes))

    def __len__(self) -> int:
        return len(self._get_runnable_keys())

    def _node_is_runnable(self, node: Node) -> bool:
        return (
            node.config.materialized in self.RUNNABLE_MATERIAL
            and node.resource_type in self.RUNNABLE_RESOURCE_TYPE
        )

    def _node_key_is_runnable(self, node_key: str, default: bool = False) -> bool:
        try:
            return self._node_is_runnable(self._manifest.nodes[node_key])
        except KeyError:
            return default

    def _get_runnable_dependencies(self, node: Node) -> List[str]:
        # Deeply traverse 'depends_on' so we catch nodes that depend on ephemerals that depends on nodes
        upstream_deps: List[str] = []
        for upstream_node_key in node.depends_on.nodes:
            up_node = self._manifest.nodes.get(upstream_node_key)
            if up_node is None:
                continue
            if self._node_is_runnable(up_node):
                upstream_deps.append(up_node.unique_id)
            else:
                upstream_deps.extend(self._get_runnable_dependencies(up_node))
        # This is a bit grim but we need the 'deep' dependencies in the Node for inserting the correct SQL literals
        # TODO: Create a NodeWrapper object that has the original Node and better dependency information
        node.depends_on.deep_nodes = upstream_deps
        return upstream_deps

    def _calculate_depths(self) -> List[List[str]]:
        remaining_nodes = self._get_runnable_keys()
        graph_data = {
            node_id: self._get_runnable_dependencies(node)
            for node_id, node in self._manifest.nodes.items()
            if node_id in remaining_nodes
        }
        graph = from_dict_of_lists(graph_data, create_using=DiGraph).reverse()
        return list(topological_generations(graph))
