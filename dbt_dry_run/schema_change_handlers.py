from typing import Callable, Dict, Optional

from dbt_dry_run.columns_metadata import expand_table_fields
from dbt_dry_run.exception import SchemaChangeException
from dbt_dry_run.models import OnSchemaChange, Table, TableField
from dbt_dry_run.models.dry_run_result import DryRunResult
from dbt_dry_run.models.report import DryRunStatus
from dbt_dry_run.models.table import MAX_SUPPORTED_NESTED_FIELD_DEPTH
from dbt_dry_run.schema_manipulation import (
    merge_table_fields,
    collect_flattened_field_paths,
)


def ignore_handler(dry_run_result: DryRunResult, target_table: Table) -> DryRunResult:
    return dry_run_result.replace_table(target_table)


def append_new_columns_handler(
    dry_run_result: DryRunResult, target_table: Table
) -> DryRunResult:
    if dry_run_result.table is None:
        return dry_run_result

    _assert_no_nested_fields_removed(dry_run_result.table.fields, target_table.fields)

    table_fields = merge_table_fields(
        table_1_fields=target_table.fields,
        table_2_fields=dry_run_result.table.fields,
    )

    return dry_run_result.replace_table(Table(fields=table_fields))


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

    _assert_no_nested_fields_removed(dry_run_result.table.fields, target_table.fields)

    table_fields = merge_table_fields(
        table_1_fields=target_columns_with_removed_columns,
        table_2_fields=dry_run_result.table.fields,
    )

    return dry_run_result.replace_table(Table(fields=table_fields))


def fail_handler(dry_run_result: DryRunResult, target_table: Table) -> DryRunResult:
    if dry_run_result.table is None:
        return dry_run_result

    predicted_table_field_names = set(expand_table_fields(dry_run_result.table))
    target_table_field_names = set(expand_table_fields(target_table))
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


def _assert_no_nested_fields_removed(
    new_fields: list[TableField], existing_fields: list[TableField]
) -> None:
    existing_fields_with_flattened_path = collect_flattened_field_paths(
        existing_fields, max_depth=MAX_SUPPORTED_NESTED_FIELD_DEPTH
    )
    new_fields_with_flattened_path = collect_flattened_field_paths(
        new_fields, max_depth=MAX_SUPPORTED_NESTED_FIELD_DEPTH
    )

    new_field_paths = set(
        new_field.path for new_field in new_fields_with_flattened_path
    )

    for field in existing_fields_with_flattened_path:
        if (
            field.path is not None
            and field.path not in new_field_paths
            and not field.is_top_level
        ):
            raise SchemaChangeException(
                f"Field '{'.'.join(field.path)}' has been removed from a nested column"
            )
