from dbt_dry_run.models.report import DryRunStatus
from integration.conftest import CompletedDryRun
from integration.utils import (
    get_report_node_by_id,
)


def test_compiled_models_pass(
    dry_run_result_skip_not_compiled: CompletedDryRun,
) -> None:
    mart_node = get_report_node_by_id(
        dry_run_result_skip_not_compiled.get_report(),
        "model.test_partially_compiled_project.run_mart_model",
    )

    staging_node = get_report_node_by_id(
        dry_run_result_skip_not_compiled.get_report(),
        "model.test_partially_compiled_project.run_staging_model",
    )
    skipped_test_node = get_report_node_by_id(
        dry_run_result_skip_not_compiled.get_report(),
        "test.test_partially_compiled_project.run_mart_model_excluded_test",
    )

    assert mart_node.success
    assert staging_node.success
    assert skipped_test_node.status == DryRunStatus.SKIPPED


def test_not_compiled_models_pass(
    dry_run_result_skip_not_compiled: CompletedDryRun,
) -> None:
    staging_node = get_report_node_by_id(
        dry_run_result_skip_not_compiled.get_report(),
        "model.test_partially_compiled_project.skip_staging_model",
    )
    mart_node = get_report_node_by_id(
        dry_run_result_skip_not_compiled.get_report(),
        "model.test_partially_compiled_project.skip_mart_model",
    )
    assert staging_node.status == DryRunStatus.SKIPPED
    assert mart_node.status == DryRunStatus.FAILURE
    assert mart_node.error_message == "UpstreamFailedException"


# TODO: We want to assert that models downstream of `skip_staging_model` fail
# TODO: Also want to add a case for a 'dangling' skipped model that shows it does not cause failure
