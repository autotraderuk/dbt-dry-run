from integration.conftest import ProjectContext
from integration.utils import (
    assert_report_node_has_columns,
    get_report_node_by_id,
    assert_node_failed_with_error,
)


def test_single_column_ignore_retains_schema_in_target(
    compiled_project: ProjectContext,
):
    node_id = "model.test_incremental.single_column_ignore"
    manifest_node = compiled_project.manifest.nodes[node_id]
    columns = ["my_string2 STRING"]
    with compiled_project.create_state(manifest_node, columns):
        run_result = compiled_project.dry_run()
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
        report_node = get_report_node_by_id(
            run_result.report,
            node_id,
        )
        assert_report_node_has_columns(report_node, {"my_string", "my_string2"})


def test_single_column_ignore_raises_error_if_column_type_changes(
    compiled_project: ProjectContext,
):
    node_id = "model.test_incremental.single_column_ignore"
    manifest_node = compiled_project.manifest.nodes[node_id]
    columns = ["my_string NUMERIC"]
    with compiled_project.create_state(manifest_node, columns):
        run_result = compiled_project.dry_run()
        assert_node_failed_with_error(run_result.report, node_id, "BadRequest")
