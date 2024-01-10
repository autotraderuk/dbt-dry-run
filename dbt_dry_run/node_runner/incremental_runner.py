from textwrap import dedent
from typing import Callable, Dict, Iterable, Optional, Set

from dbt_dry_run import flags
from dbt_dry_run.exception import SchemaChangeException, UpstreamFailedException
from dbt_dry_run.literals import insert_dependant_sql_literals
from dbt_dry_run.models import Table
from dbt_dry_run.models.manifest import Node, OnSchemaChange
from dbt_dry_run.node_runner import NodeRunner
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
        node=dry_run_result.node, table=table, status=status, exception=exception
    )


ON_SCHEMA_CHANGE_TABLE_HANDLER: Dict[
    OnSchemaChange, Callable[[DryRunResult, Table], DryRunResult]
] = {
    OnSchemaChange.IGNORE: ignore_handler,
    OnSchemaChange.APPEND_NEW_COLUMNS: append_new_columns_handler,
    OnSchemaChange.SYNC_ALL_COLUMNS: sync_all_columns_handler,
    OnSchemaChange.FAIL: fail_handler,
}

PARTITION_DATA_TYPES_VALUES_MAPPING: Dict[str, str] = {
    "timestamp": "CURRENT_TIMESTAMP()",
    "datetime": "CURRENT_DATETIME()",
    "date": "CURRENT_DATE()",
    "int64": "100",
}


def get_common_field_names(left: Table, right: Table) -> Set[str]:
    return left.field_names.intersection(right.field_names)


def get_merge_sql(
    node: Node, common_field_names: Iterable[str], select_statement: str
) -> str:
    values_csv = ",".join(sorted(common_field_names))
    return dedent(
        f"""MERGE {node.to_table_ref_literal()}
                USING (
                  {select_statement}
                )
                ON False
                WHEN NOT MATCHED THEN 
                INSERT ({values_csv}) 
                VALUES ({values_csv})
            """
    )


def sql_has_recursive_ctes(code: str) -> bool:
    code_tokens = code.lower().split()
    for index in range(0, len(code_tokens) - 1):
        if code_tokens[index : index + 2] == ["with", "recursive"]:
            return True
    return False


class IncrementalRunner(NodeRunner):
    resource_type = ("model",)

    def _verify_merge_type_compatibility(
        self,
        node: Node,
        sql_statement: str,
        initial_result: DryRunResult,
        target_table: Table,
    ) -> DryRunResult:
        if not initial_result.table or sql_has_recursive_ctes(node.compiled_code):
            return initial_result
        common_field_names = get_common_field_names(initial_result.table, target_table)
        if not common_field_names:
            return initial_result
        sql_statement = get_merge_sql(node, common_field_names, sql_statement)
        status, model_schema, exception = self._sql_runner.query(sql_statement)
        if status == DryRunStatus.SUCCESS:
            return initial_result
        else:
            return DryRunResult(node, None, status, exception)

    def _modify_sql(self, node: Node, sql_statement: str) -> str:
        if node.config.sql_header:
            sql_statement = f"{node.config.sql_header}\n{sql_statement}"

        if node.config.partition_by and "_dbt_max_partition" in node.compiled_code:
            dbt_max_partition_declaration = (
                f"declare _dbt_max_partition {node.config.partition_by.data_type} default"
                f" {PARTITION_DATA_TYPES_VALUES_MAPPING[node.config.partition_by.data_type]};"
            )
            sql_statement = f"{dbt_max_partition_declaration}\n{sql_statement}"

        return sql_statement

    def _get_full_refresh_config(self, node: Node) -> bool:
        # precedence defined here - https://docs.getdbt.com/reference/resource-configs/full_refresh
        if node.config.full_refresh is not None:
            return node.config.full_refresh
        return flags.FULL_REFRESH

    def run(self, node: Node) -> DryRunResult:
        try:
            run_sql = insert_dependant_sql_literals(node, self._results)
        except UpstreamFailedException as e:
            return DryRunResult(node, None, DryRunStatus.FAILURE, e)

        run_sql = self._modify_sql(node, run_sql)
        status, model_schema, exception = self._sql_runner.query(run_sql)

        result = DryRunResult(node, model_schema, status, exception)

        full_refresh = self._get_full_refresh_config(node)

        if result.status == DryRunStatus.SUCCESS and not full_refresh:
            target_table = self._sql_runner.get_node_schema(node)
            if target_table:
                on_schema_change = node.config.on_schema_change or OnSchemaChange.IGNORE
                handler = ON_SCHEMA_CHANGE_TABLE_HANDLER[on_schema_change]
                result = self._verify_merge_type_compatibility(
                    node, run_sql, result, target_table
                )
                if result.status == DryRunStatus.SUCCESS:
                    result = handler(result, target_table)

        return result
