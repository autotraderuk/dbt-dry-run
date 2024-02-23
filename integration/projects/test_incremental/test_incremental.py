from integration.conftest import ProjectContext
from integration.utils import (
    assert_report_node_has_columns,
    get_report_node_by_id,
    assert_node_failed_with_error,
    assert_report_produced,
    assert_report_node_has_columns_in_order,
)


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


def test_single_struct_column_append_new_columns_fails_to_add_new_field(
    compiled_project: ProjectContext,
):
    node_id = "model.test_incremental.single_struct_column_append_new_columns"
    manifest_node = compiled_project.manifest.nodes[node_id]
    columns = ["my_struct STRUCT<my_string STRING>"]
    with compiled_project.create_state(manifest_node, columns):
        run_result = compiled_project.dry_run()
        assert_report_produced(run_result)
        assert_node_failed_with_error(run_result.report, node_id, "BadRequest")


def test_cli_full_refresh_should_use_the_model_schema(
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


def test_cli_full_refresh_with_full_refresh_set_to_false_on_the_model_use_the_target_schema(
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


def test_full_refresh_on_incremental_model_should_use_the_model_schema(
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


def test_no_full_refresh_on_the_model_use_the_target_schema(
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


def test_column_order_preserved_on_schema_change_ignore(
    compiled_project: ProjectContext,
):
    node_id = "model.test_incremental.column_order_preserved_osc_ignore"
    manifest_node = compiled_project.manifest.nodes[node_id]
    columns = ["col_2 STRING", "col_1 STRING"]
    with compiled_project.create_state(manifest_node, columns):
        run_result = compiled_project.dry_run()
        assert_report_produced(run_result)
        report_node = get_report_node_by_id(
            run_result.report,
            node_id,
        )
        assert_report_node_has_columns_in_order(report_node, ["col_2", "col_1"])


def test_recursive_cte_does_not_check_merge_compatibility(
    compiled_project: ProjectContext,
):
    node_id = "model.test_incremental.recursive_cte"
    manifest_node = compiled_project.manifest.nodes[node_id]
    columns = ["my_string NUMERIC"]
    with compiled_project.create_state(manifest_node, columns):
        run_result = compiled_project.dry_run()
        assert_report_produced(run_result)
        report_node = get_report_node_by_id(
            run_result.report,
            node_id,
        )
        assert_report_node_has_columns(report_node, {"my_string"})


def test_column_order_preserved_on_schema_change_append_new_columns(
    compiled_project: ProjectContext,
):
    node_id = "model.test_incremental.column_order_preserved_osc_append"
    manifest_node = compiled_project.manifest.nodes[node_id]
    columns = ["col_2 STRING", "col_1 STRING"]
    with compiled_project.create_state(manifest_node, columns):
        run_result = compiled_project.dry_run()
        assert_report_produced(run_result)
        report_node = get_report_node_by_id(
            run_result.report,
            node_id,
        )
        assert_report_node_has_columns_in_order(report_node, ["col_2", "col_1"])


def test_required_partition_filter(
    compiled_project: ProjectContext,
):
    node_id = "model.test_incremental.required_partition_filter"
    manifest_node = compiled_project.manifest.nodes[node_id]
    columns = ["col_1 STRING", "col_2 STRING", "snapshot_date DATE"]
    with compiled_project.create_state(manifest_node, columns, "snapshot_date", True):
        run_result = compiled_project.dry_run()
        assert_report_produced(run_result)
        report_node = get_report_node_by_id(
            run_result.report,
            node_id,
        )
        assert_report_node_has_columns_in_order(
            report_node, ["col_1", "col_2", "snapshot_date"]
        )


def test_sql_header_and_max_partition(
    compiled_project: ProjectContext,
):
    node_id = "model.test_incremental.with_sql_header_and_dbt_max_partition"
    manifest_node = compiled_project.manifest.nodes[node_id]
    columns = ["snapshot_date", "my_string STRING", "my_func_output STRING"]
    with compiled_project.create_state(manifest_node, columns, "snapshot_date", True):
        run_result = compiled_project.dry_run()
        assert_report_produced(run_result)
        report_node = get_report_node_by_id(
            run_result.report,
            node_id,
        )
        assert_report_node_has_columns_in_order(
            report_node, ["snapshot_date", "my_string", "my_func_output"]
        )
