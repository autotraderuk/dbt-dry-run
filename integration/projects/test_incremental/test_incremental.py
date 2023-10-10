import pytest

from integration.conftest import ProjectContext
from integration.utils import (
    assert_report_node_has_columns,
    get_report_node_by_id,
    assert_node_failed_with_error,
    assert_report_produced, )


def test_single_column_ignore_retains_schema_in_target(
    compiled_project: ProjectContext,
):
    node_id = "model.test_incremental.single_column_ignore"
    manifest_node = compiled_project.manifest.nodes[node_id]
    columns = ["my_string2 STRING"]
    with compiled_project.create_state(manifest_node, columns):
        run_result = compiled_project.dry_run()
        assert_report_produced(run_result)
        report_node = get_report_node_by_id(
            run_result.report,
            node_id,
        )
        assert_report_node_has_columns(report_node, {"my_string2"})


def test_single_column_append_new_columns_has_both_columns(
    compiled_project: ProjectContext,
):
    node_id = "model.test_incremental.single_column_append_new_columns"
    manifest_node = compiled_project.manifest.nodes[node_id]
    columns = ["my_string2 STRING"]
    with compiled_project.create_state(manifest_node, columns):
        run_result = compiled_project.dry_run()
        assert_report_produced(run_result)
        report_node = get_report_node_by_id(
            run_result.report,
            node_id,
        )
        assert_report_node_has_columns(report_node, {"my_string", "my_string2"})


@pytest.mark.xfail(
    reason="False positive if column type of incremental changes: https://github.com/autotraderuk/dbt-dry-run/issues/26"
)
def test_single_column_ignore_raises_error_if_column_type_changes(
    compiled_project: ProjectContext,
):
    node_id = "model.test_incremental.single_column_ignore"
    manifest_node = compiled_project.manifest.nodes[node_id]
    columns = ["my_string NUMERIC"]
    with compiled_project.create_state(manifest_node, columns):
        run_result = compiled_project.dry_run()
        assert_report_produced(run_result)
        assert_node_failed_with_error(run_result.report, node_id, "BadRequest")


def test_cli_full_refresh_should_pass_dry_run_using_the_model_schema(
    compiled_project_full_refresh: ProjectContext,
):
    node_id = "model.test_incremental.double_column_none_full_refresh"
    manifest_node = compiled_project_full_refresh.manifest.nodes[node_id]
    columns = ["existing_column STRING"]
    with compiled_project_full_refresh.create_state(manifest_node, columns):
        run_result = compiled_project_full_refresh.dry_run(full_refresh=True)
        assert_report_produced(run_result)
        report_node = get_report_node_by_id(
            run_result.report,
            node_id,
        )
        assert_report_node_has_columns(report_node, {"existing_column", "new_column"})


def test_cli_full_refresh_with_full_refresh_set_to_false_on_the_model_should_fail_dry_run_using_the_bigquery_schema(
    compiled_project_full_refresh: ProjectContext,
):
    node_id = "model.test_incremental.double_column_explicit_no_full_refresh"
    manifest_node = compiled_project_full_refresh.manifest.nodes[node_id]
    columns = ["existing_column STRING"]
    with compiled_project_full_refresh.create_state(manifest_node, columns):
        run_result = compiled_project_full_refresh.dry_run(full_refresh=True)
        assert_report_produced(run_result)
        report_node = get_report_node_by_id(
            run_result.report,
            node_id,
        )
        assert_report_node_has_columns(report_node, {"existing_column"})


def test_full_refresh_on_incremental_model_should_pass_dry_run_using_the_model_schema(
    compiled_project: ProjectContext,
):
    node_id = "model.test_incremental.double_column_model_full_refresh"
    manifest_node = compiled_project.manifest.nodes[node_id]
    columns = ["existing_column STRING"]
    with compiled_project.create_state(manifest_node, columns):
        run_result = compiled_project.dry_run()
        assert_report_produced(run_result)
        report_node = get_report_node_by_id(
            run_result.report,
            node_id,
        )
        assert_report_node_has_columns(report_node, {"existing_column", "new_column"})


def test_no_full_refresh_on_the_model_should_fail_dry_run_using_the_bigquery_schema(
    compiled_project: ProjectContext,
):
    node_id = "model.test_incremental.double_column_explicit_no_full_refresh"
    manifest_node = compiled_project.manifest.nodes[node_id]
    columns = ["existing_column STRING"]
    with compiled_project.create_state(manifest_node, columns):
        run_result = compiled_project.dry_run()
        assert_report_produced(run_result)
        report_node = get_report_node_by_id(
            run_result.report,
            node_id,
        )
        assert_report_node_has_columns(report_node, {"existing_column"})

