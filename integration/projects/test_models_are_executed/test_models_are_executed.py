from integration.conftest import IntegrationTestResult
from integration.utils import assert_report_success, get_report_node_by_id, assert_report_node_has_columns


def test_success(dry_run_result: IntegrationTestResult):
    assert_report_success(dry_run_result)


def test_ran_correct_number_of_nodes(dry_run_result: IntegrationTestResult):
    report = assert_report_success(dry_run_result)
    assert report.node_count == 4


def test_table_of_nodes_is_returned(dry_run_result: IntegrationTestResult):
    report = assert_report_success(dry_run_result)
    seed_node = get_report_node_by_id(report, "seed.test_models_are_executed.my_seed")
    assert_report_node_has_columns(seed_node, {"a", "seed_b"})

    first_layer = get_report_node_by_id(report, "model.test_models_are_executed.first_layer")
    assert_report_node_has_columns(first_layer, {"a", "b", "c"})

    second_layer = get_report_node_by_id(report, "model.test_models_are_executed.second_layer")
    assert_report_node_has_columns(second_layer, {"a", "b", "c", "seed_b"})


def test_disabled_model_not_run(dry_run_result: IntegrationTestResult):
    report = assert_report_success(dry_run_result)
    assert "model.test_models_are_executed.disabled_model" not in set(
        n.unique_id for n in report.nodes), "Found disabled model in dry run output"
