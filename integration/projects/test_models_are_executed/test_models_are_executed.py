import pytest

from dbt_dry_run.columns_metadata import expand_table_fields_with_types
from dbt_dry_run.models import BigQueryFieldType
from dbt_dry_run.results import DryRunStatus
from integration.conftest import DryRunResult
from integration.utils import (
    get_report_node_by_id,
    assert_report_node_has_columns,
    assert_report_produced,
)


def test_ran_correct_number_of_nodes(dry_run_result: DryRunResult):
    report = assert_report_produced(dry_run_result)
    assert report.node_count == 5


def test_table_of_nodes_is_returned(dry_run_result: DryRunResult):
    report = assert_report_produced(dry_run_result)
    seed_node = get_report_node_by_id(report, "seed.test_models_are_executed.my_seed")
    columns = expand_table_fields_with_types(seed_node.table)
    assert columns == {
        "a": BigQueryFieldType.STRING,
        "seed_b": BigQueryFieldType.FLOAT64,
        "seed_c": BigQueryFieldType.BIGNUMERIC,
    }

    first_layer = get_report_node_by_id(
        report, "model.test_models_are_executed.first_layer"
    )
    assert_report_node_has_columns(first_layer, {"a", "b", "c"})

    second_layer = get_report_node_by_id(
        report, "model.test_models_are_executed.second_layer"
    )
    assert_report_node_has_columns(second_layer, {"a", "b", "c", "seed_b", "seed_c"})


def test_disabled_model_not_run(dry_run_result: DryRunResult):
    report = assert_report_produced(dry_run_result)
    assert "model.test_models_are_executed.disabled_model" not in set(
        n.unique_id for n in report.nodes
    ), "Found disabled model in dry run output"


@pytest.mark.xfail(
    reason="Seed type compatibility not checked. (Trying to convert string to number)"
)
def test_badly_configured_seed_fails(dry_run_result: DryRunResult):
    report = assert_report_produced(dry_run_result)
    seed_node = get_report_node_by_id(
        report, "seed.test_models_are_executed.badly_configured_seed"
    )
    assert seed_node.status == DryRunStatus.FAILURE


def test_model_with_all_column_types_succeeds(dry_run_result: DryRunResult):
    node = get_report_node_by_id(
        dry_run_result.report,
        "model.test_models_are_executed.model_with_all_column_types",
    )
    expected_column_names = {
        "my_string",
        "my_bytes",
        "my_integer",
        "my_int64",
        "my_float",
        "my_float64",
        "my_boolean",
        "my_bool",
        "my_timestamp",
        "my_date",
        "my_time",
        "my_datetime",
        "my_interval",
        "my_geography",
        "my_numeric",
        "my_bignumeric",
        "my_json",
        "my_struct",
        "my_struct.field_1",
        "my_struct.field_2",
        "my_struct.field_3",
        "my_struct.field_3.field_3_sub_field_1",
        "my_struct.field_3.field_3_sub_field_2",
        "my_array_of_records",
        "my_array_of_records.col_1",
        "my_array_of_records.col_2",
        "my_range",
    }
    assert_report_node_has_columns(node, expected_column_names)
