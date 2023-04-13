import pytest

from dbt_dry_run.models.manifest import Node, NodeConfig, NodeMeta, PartitionBy
from dbt_dry_run.test.utils import SimpleNode


def test_partition_by_config_case_insensitive() -> None:
    partition_by = PartitionBy(field="a_field", data_type="TIMESTAMP")
    assert partition_by.field == "a_field"
    assert partition_by.data_type == "timestamp"


def test_node_get_combined_metadata_inherits_from_node_meta() -> None:
    config = NodeConfig(materialized="table", meta=None)
    node = SimpleNode(
        unique_id="a",
        depends_on=[],
        table_config=config,
        meta=NodeMeta.parse_obj({NodeMeta.DEFAULT_CHECK_COLUMNS_KEY: True}),
    ).to_node()

    assert node.get_combined_metadata(NodeMeta.DEFAULT_CHECK_COLUMNS_KEY) is True


@pytest.mark.parametrize("config_meta", [False, True])
def test_node_get_combined_metadata_is_overridden_by_config(
    config_meta: bool,
) -> None:
    config = NodeConfig(
        materialized="table",
        meta=NodeMeta.parse_obj({NodeMeta.DEFAULT_CHECK_COLUMNS_KEY: config_meta}),
    )
    node = SimpleNode(
        unique_id="a",
        depends_on=[],
        table_config=config,
        meta=NodeMeta.parse_obj({NodeMeta.DEFAULT_CHECK_COLUMNS_KEY: not config_meta}),
    ).to_node()

    assert node.get_combined_metadata(NodeMeta.DEFAULT_CHECK_COLUMNS_KEY) is config_meta


def test_metadata_parses_check_columns() -> None:
    metadata = NodeMeta.parse_obj({NodeMeta.DEFAULT_CHECK_COLUMNS_KEY: False})
    assert metadata[NodeMeta.DEFAULT_CHECK_COLUMNS_KEY] is False


def test_metadata_contains_key() -> None:
    metadata = NodeMeta.parse_obj({"my_key": False})
    assert ("my_key" in metadata) is True
