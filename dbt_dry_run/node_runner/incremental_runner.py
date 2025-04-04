from typing import Set

from dbt_dry_run import flags
from dbt_dry_run.exception import UpstreamFailedException
from dbt_dry_run.models import BigQueryFieldMode, BigQueryFieldType, Table, TableField
from dbt_dry_run.models.manifest import Node, OnSchemaChange
from dbt_dry_run.node_runner import NodeRunner
from dbt_dry_run.node_runner.schema_change_handlers import (
    ON_SCHEMA_CHANGE_TABLE_HANDLER,
)
from dbt_dry_run.results import DryRunResult, DryRunStatus
from dbt_dry_run.sql.literals import get_sql_literal_from_table
from dbt_dry_run.sql.parsing import get_merge_sql, sql_has_recursive_ctes
from dbt_dry_run.sql.statements import (
    SQLPreprocessor,
    add_dbt_max_partition_declaration,
    add_sql_header,
)


def get_common_field_names(left: Table, right: Table) -> Set[str]:
    return left.field_names.intersection(right.field_names)


class IncrementalRunner(NodeRunner):
    resource_type = ("model",)

    def _verify_merge_type_compatibility(
        self,
        node: Node,
        initial_result: DryRunResult,
        target_table: Table,
    ) -> DryRunResult:
        if not initial_result.table or sql_has_recursive_ctes(node.compiled_code):
            return initial_result
        common_field_names = get_common_field_names(initial_result.table, target_table)
        if not common_field_names:
            return initial_result
        select_literal = get_sql_literal_from_table(initial_result.table)
        sql_statement_with_merge = get_merge_sql(
            node.table_ref, common_field_names, select_literal
        )
        status, model_schema, exception = self._sql_runner.query(
            sql_statement_with_merge
        )
        if status == DryRunStatus.SUCCESS:
            return initial_result
        else:
            return DryRunResult(node, None, status, exception)

    def _get_full_refresh_config(self, node: Node) -> bool:
        # precedence defined here - https://docs.getdbt.com/reference/resource-configs/full_refresh
        if node.config.full_refresh is not None:
            return node.config.full_refresh
        return flags.FULL_REFRESH

    def _is_time_ingestion_partitioned(self, node: Node) -> bool:
        if node.config.partition_by:
            if node.config.partition_by.time_ingestion_partitioning is True:
                return True
        return False

    def _replace_partition_with_time_ingestion_column(
        self, dry_run_result: DryRunResult
    ) -> DryRunResult:
        if not dry_run_result.table:
            return dry_run_result

        if not dry_run_result.node.config.partition_by:
            return dry_run_result

        new_partition_field = TableField(
            name="_PARTITIONTIME",
            type=BigQueryFieldType.TIMESTAMP,
            mode=BigQueryFieldMode.NULLABLE,
        )

        final_fields = [field for field in dry_run_result.table.fields]
        final_fields.append(new_partition_field)

        return dry_run_result.replace_table(Table(fields=final_fields))

    def run(self, node: Node) -> DryRunResult:
        try:
            run_sql = SQLPreprocessor(
                self._results, [add_sql_header, add_dbt_max_partition_declaration]
            )(node)
        except UpstreamFailedException as e:
            return DryRunResult(node, None, DryRunStatus.FAILURE, e)

        status, model_schema, exception = self._sql_runner.query(run_sql)

        result = DryRunResult(node, model_schema, status, exception)

        full_refresh = self._get_full_refresh_config(node)

        if result.status == DryRunStatus.SUCCESS and not full_refresh:
            target_table = self._sql_runner.get_node_schema(node)
            if target_table:
                result = self._verify_merge_type_compatibility(
                    node, result, target_table
                )
                on_schema_change = node.config.on_schema_change or OnSchemaChange.IGNORE
                handler = ON_SCHEMA_CHANGE_TABLE_HANDLER[on_schema_change]
                if result.status == DryRunStatus.SUCCESS:
                    result = handler(result, target_table)

        if (
            result.status == DryRunStatus.SUCCESS
            and self._is_time_ingestion_partitioned(node)
        ):
            result = self._replace_partition_with_time_ingestion_column(result)

        return result
