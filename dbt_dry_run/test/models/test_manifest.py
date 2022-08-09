from dbt_dry_run.models.manifest import PartitionBy


def test_partition_by_config_case_insensitive() -> None:
    partition_by = PartitionBy(field="a_field", data_type="TIMESTAMP")
    assert partition_by.field == "a_field"
    assert partition_by.data_type == "timestamp"
