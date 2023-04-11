from itertools import chain
from typing import Dict, Iterator, List, Optional, Set

from networkx import DiGraph, from_dict_of_lists, topological_generations

from dbt_dry_run.models.manifest import Manifest, Node


class ManifestScheduler:
    MODEL = "model"
    SEED = "seed"
    SNAPSHOT = "snapshot"
    SOURCE = "source"
    TEST = "test"
    RUNNABLE_RESOURCE_TYPE = (MODEL)
    RUNNABLE_MATERIAL = ("view", "table", "incremental", "seed", "snapshot", "test")

    def __init__(self, manifest: Manifest, tags: Optional[str] = None):
        self._manifest = manifest
        self._tags_filter = tags
        self._status: Dict[str, bool] = {
            node_key: False for node_key in self._manifest.all_nodes.keys()
        }

    def _filter_manifest(self) -> Set[str]:
        if self._tags_filter is None:
            return set(self._manifest.all_nodes.keys())
        else:
            check_tags = self._tags_filter.split(",")
            tags_checklist = check_tags.copy()
            filtered_nodes = []
            for key, node in self._manifest.all_nodes.items():
                node_resource_type = node.resource_type
                node_tags = node.tags
                if node_resource_type in self.RUNNABLE_RESOURCE_TYPE:
                    for tag in node_tags:
                        if tag in check_tags:
                            filtered_nodes.append(key)
                            try:
                                tags_checklist.remove(tag)
                            except:
                                pass # tags already remove from check list
            if tags_checklist == []:
                return set(filtered_nodes)
            else:
                raise ValueError(f"Unknown tags: {tags_checklist}")

    def _get_runnable_keys(self) -> Set[str]:
        remaining_nodes = set(
            filter(self._node_key_is_runnable, self._manifest.all_nodes.keys())
        )

        if self._tags_filter:
            remaining_nodes = remaining_nodes.intersection(self._filter_manifest())
        return remaining_nodes

    def __iter__(self) -> Iterator[List[Node]]:
        generation: List[str]
        for gen_id, generation in enumerate(self._calculate_depths()):
            nodes = [
                self._manifest.all_nodes[k]
                for k in generation
                if k in self._manifest.all_nodes
            ]
            yield nodes  # list(filter(self._node_is_runnable, nodes))

    def __len__(self) -> int:
        return len(self._get_runnable_keys())

    def _node_is_runnable(self, node: Node) -> bool:
        node_is_runnable_type = (
            node.resource_type in self.RUNNABLE_RESOURCE_TYPE
            and node.config.materialized in self.RUNNABLE_MATERIAL
        ) or (node.is_external_source())
        return node.config.enabled and node_is_runnable_type

    def _node_key_is_runnable(self, node_key: str, default: bool = False) -> bool:
        try:
            return self._node_is_runnable(self._manifest.all_nodes[node_key])
        except KeyError:
            return default

    def _get_runnable_dependencies(self, node: Node) -> List[str]:
        # Deeply traverse 'depends_on' so we catch nodes that depend on ephemerals that depends on nodes
        upstream_deps: List[str] = []
        for upstream_node_key in node.depends_on.nodes:
            up_node = self._manifest.all_nodes.get(upstream_node_key)
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
            for node_id, node in self._manifest.all_nodes.items()
            if node_id in remaining_nodes
        }
        graph = from_dict_of_lists(graph_data, create_using=DiGraph).reverse()
        return list(topological_generations(graph))
