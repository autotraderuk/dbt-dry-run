from typing import Callable, Dict, Optional

from dbt_dry_run.exception import SchemaChangeException
from dbt_dry_run.models import OnSchemaChange, Table
from dbt_dry_run.models.dry_run_result import DryRunResult
from dbt_dry_run.models.report import DryRunStatus
from dbt_dry_run.nested_schema_change import (
    get_model_fields_not_present_in_target,
    add_new_model_fields_to_target_table,
    assert_model_removes_no_nested_fields_from_target,
)


def ignore_handler(dry_run_result: DryRunResult, target_table: Table) -> DryRunResult:
    return dry_run_result.replace_table(target_table)


def append_new_columns_handler(
    dry_run_result: DryRunResult, target_table: Table
) -> DryRunResult:
    if dry_run_result.table is None:
        return dry_run_result

    missing_fields = get_model_fields_not_present_in_target(
        dry_run_result.table.fields, target_table.fields
    )
    assert_model_removes_no_nested_fields_from_target(
        dry_run_result.table.fields, target_table.fields
    )
    final_fields = add_new_model_fields_to_target_table(target_table, missing_fields)
    return dry_run_result.replace_table(Table(fields=final_fields))


def sync_all_columns_handler(
    dry_run_result: DryRunResult, target_table: Table
) -> DryRunResult:
    if dry_run_result.table is None:
        return dry_run_result

    dry_run_column_names = set(field.name for field in dry_run_result.table.fields)
    target_columns_with_removed_columns = [
        existing_field
        for existing_field in target_table.fields
        if existing_field.name in dry_run_column_names
    ]
    model_fields_not_present_in_target = get_model_fields_not_present_in_target(
        dry_run_result.table.fields, target_columns_with_removed_columns
    )

    ## Should only remove top level columns that are not present in the model
    assert_model_removes_no_nested_fields_from_target(
        dry_run_result.table.fields, target_columns_with_removed_columns
    )
    final_fields = add_new_model_fields_to_target_table(
        target_table=Table(fields=target_columns_with_removed_columns),
        new_fields_from_model=model_fields_not_present_in_target,
    )

    return dry_run_result.replace_table(Table(fields=final_fields))


def fail_handler(dry_run_result: DryRunResult, target_table: Table) -> DryRunResult:
    if dry_run_result.table is None:
        return dry_run_result
    predicted_table_field_names = set(
        [field.name for field in dry_run_result.table.fields]
    )
    target_table_field_names = set([field.name for field in target_table.fields])
    added_fields = predicted_table_field_names.difference(target_table_field_names)
    removed_fields = target_table_field_names.difference(predicted_table_field_names)
    schema_changed = added_fields or removed_fields
    table: Optional[Table] = target_table
    status = dry_run_result.status
    exception = dry_run_result.exception
    if schema_changed:
        table = None
        status = DryRunStatus.FAILURE
        msg = (
            f"Incremental model has changed schemas. "
            f"Fields added: {added_fields}, "
            f"Fields removed: {removed_fields}"
        )
        exception = SchemaChangeException(msg)
    return DryRunResult(
        node=dry_run_result.node,
        table=table,
        status=status,
        exception=exception,
    )


ON_SCHEMA_CHANGE_TABLE_HANDLER: Dict[
    OnSchemaChange, Callable[[DryRunResult, Table], DryRunResult]
] = {
    OnSchemaChange.IGNORE: ignore_handler,
    OnSchemaChange.APPEND_NEW_COLUMNS: append_new_columns_handler,
    OnSchemaChange.SYNC_ALL_COLUMNS: sync_all_columns_handler,
    OnSchemaChange.FAIL: fail_handler,
}
