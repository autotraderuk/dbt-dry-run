from dbt_dry_run.exception import UpstreamFailedException
from dbt_dry_run.models import BigQueryFieldMode, BigQueryFieldType, Table, TableField
from dbt_dry_run.models.manifest import Node, OnSchemaChange
from dbt_dry_run.node_runner import NodeRunner
from dbt_dry_run.results import DryRunResult, DryRunStatus
from dbt_dry_run.schema_change_handlers import ON_SCHEMA_CHANGE_TABLE_HANDLER
from dbt_dry_run.sql.literals import get_sql_literal_from_table
from dbt_dry_run.sql.parsing import get_merge_sql, sql_has_recursive_ctes
from dbt_dry_run.sql.statements import (
    SQLPreprocessor,
    add_dbt_max_partition_declaration,
    add_sql_header,
    insert_dependant_sql_literals,
)


class IncrementalRunner(NodeRunner):
    resource_type = ("model",)
    preprocessor = SQLPreprocessor(
        [
            insert_dependant_sql_literals,
            add_sql_header,
            add_dbt_max_partition_declaration,
        ]
    )

    def _verify_merge_type_compatibility(
        self,
        node: Node,
        initial_result: DryRunResult,
        target_table: Table,
    ) -> DryRunResult:
        if not initial_result.table or sql_has_recursive_ctes(node.compiled_code):
            return initial_result
        common_field_names = initial_result.table.common_field_names(target_table)
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
            run_sql = self.preprocessor(node, self._results)
        except UpstreamFailedException as e:
            return DryRunResult(node, None, DryRunStatus.FAILURE, e)

        status, model_schema, exception = self._sql_runner.query(run_sql)

        result = DryRunResult(node, model_schema, status, exception)

        if result.status == DryRunStatus.SUCCESS and not node.get_should_full_refresh():
            target_table = self._sql_runner.get_node_schema(node)
            if target_table:
                result = self._verify_merge_type_compatibility(
                    node, result, target_table
                )
                on_schema_change = node.config.on_schema_change or OnSchemaChange.IGNORE
                if result.status == DryRunStatus.SUCCESS:
                    handler = ON_SCHEMA_CHANGE_TABLE_HANDLER[on_schema_change]
                    result = handler(result, target_table)

        if result.status == DryRunStatus.SUCCESS and node.is_time_ingestion_partitioned:
            result = self._replace_partition_with_time_ingestion_column(result)

        return result
