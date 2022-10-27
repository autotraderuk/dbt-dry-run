from typing import Callable, Dict, Optional

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
    mapped_predicted_table = {
        field.name: field for field in dry_run_result.table.fields
    }
    mapped_target_table = {field.name: field for field in target_table.fields}
    mapped_predicted_table.update(mapped_target_table)
    return dry_run_result.replace_table(
        Table(fields=list(mapped_predicted_table.values()))
    )


def sync_all_columns_handler(
    dry_run_result: DryRunResult, target_table: Table
) -> DryRunResult:
    return dry_run_result


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


class ModelRunner(NodeRunner):
    resource_type = ("model",)

    def _modify_sql(self, node: Node, sql_statement: str) -> str:
        if node.config.materialized == "view":
            sql_statement = f"CREATE OR REPLACE VIEW `{node.database}`.`{node.db_schema}`.`{node.alias}` AS (\n{sql_statement}\n)"

        if node.config.sql_header:
            sql_statement = f"{node.config.sql_header}\n{sql_statement}"

        if (
            node.config.materialized == "incremental"
            and node.config.partition_by
            and "_dbt_max_partition" in node.compiled_code
        ):
            dbt_max_partition_declaration = (
                f"declare _dbt_max_partition {node.config.partition_by.data_type} default"
                f" {PARTITION_DATA_TYPES_VALUES_MAPPING[node.config.partition_by.data_type]};"
            )
            sql_statement = f"{dbt_max_partition_declaration}\n{sql_statement}"

        return sql_statement

    def run(self, node: Node) -> DryRunResult:
        try:
            run_sql = insert_dependant_sql_literals(node, self._results)
        except UpstreamFailedException as e:
            return DryRunResult(node, None, DryRunStatus.FAILURE, e)

        run_sql = self._modify_sql(node, run_sql)
        status, predicted_table, exception = self._sql_runner.query(run_sql)

        result = DryRunResult(node, predicted_table, status, exception)

        if (
            result.status == DryRunStatus.SUCCESS
            and node.config.materialized == "incremental"
        ):
            target_table = self._sql_runner.get_node_schema(node)
            if target_table:
                on_schema_change = node.config.on_schema_change or OnSchemaChange.IGNORE
                handler = ON_SCHEMA_CHANGE_TABLE_HANDLER[on_schema_change]
                result = handler(result, target_table)

        return result
