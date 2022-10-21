import pytest

from dbt_dry_run.models.manifest import Node, NodeConfig, NodeMeta, PartitionBy
from dbt_dry_run.test.utils import SimpleNode


def test_partition_by_config_case_insensitive() -> None:
    partition_by = PartitionBy(field="a_field", data_type="TIMESTAMP")
    assert partition_by.field == "a_field"
    assert partition_by.data_type == "timestamp"


def test_node_get_should_check_columns_defaults_to_false() -> None:
    config = NodeConfig(materialized="table", meta=None)
    node = SimpleNode(
        unique_id="a", depends_on=[], table_config=config, meta=None
    ).to_node()

    assert node.get_should_check_columns() is False


def test_node_get_should_check_columns_inherits_from_node_meta() -> None:
    config = NodeConfig(materialized="table", meta=None)
    node = SimpleNode(
        unique_id="a",
        depends_on=[],
        table_config=config,
        meta=NodeMeta(check_columns=True),
    ).to_node()

    assert node.get_should_check_columns() is True


@pytest.mark.parametrize("config_meta", [False, True])
def test_node_get_should_check_columns_is_overriden_by_config(
    config_meta: bool,
) -> None:
    config = NodeConfig(materialized="table", meta=NodeMeta(check_columns=config_meta))
    node = SimpleNode(
        unique_id="a",
        depends_on=[],
        table_config=config,
        meta=NodeMeta(check_columns=not config_meta),
    ).to_node()

    assert node.get_should_check_columns() is config_meta
