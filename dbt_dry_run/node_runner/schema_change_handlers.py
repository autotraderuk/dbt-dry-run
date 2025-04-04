from typing import Callable, Dict, Optional

from dbt_dry_run.exception import SchemaChangeException
from dbt_dry_run.models import OnSchemaChange, Table
from dbt_dry_run.results import DryRunResult, DryRunStatus


def ignore_handler(dry_run_result: DryRunResult, target_table: Table) -> DryRunResult:
    return dry_run_result.replace_table(target_table)


def append_new_columns_handler(
    dry_run_result: DryRunResult, target_table: Table
) -> DryRunResult:
    if dry_run_result.table is None:
        return dry_run_result

    target_column_names = set(field.name for field in target_table.fields)
    new_columns = [
        new_field
        for new_field in dry_run_result.table.fields
        if new_field.name not in target_column_names
    ]
    final_fields = target_table.fields + new_columns
    return dry_run_result.replace_table(Table(fields=final_fields))


def sync_all_columns_handler(
    dry_run_result: DryRunResult, target_table: Table
) -> DryRunResult:
    if dry_run_result.table is None:
        return dry_run_result
    predicted_column_names = set(field.name for field in dry_run_result.table.fields)
    target_column_names = set(field.name for field in target_table.fields)
    new_columns = [
        new_field
        for new_field in dry_run_result.table.fields
        if new_field.name not in target_column_names
    ]
    existing_columns = [
        existing_field
        for existing_field in target_table.fields
        if existing_field.name in predicted_column_names
    ]
    final_fields = existing_columns + new_columns
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
