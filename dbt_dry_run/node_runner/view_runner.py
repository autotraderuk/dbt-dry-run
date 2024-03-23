from dbt_dry_run.exception import UpstreamFailedException
from dbt_dry_run.literals import insert_dependant_sql_literals
from dbt_dry_run.models.manifest import Node
from dbt_dry_run.node_runner import NodeRunner
from dbt_dry_run.results import DryRunResult, DryRunStatus


class ViewRunner(NodeRunner):
    def _modify_sql(self, node: Node, sql_statement: str) -> str:
        sql_statement = f"CREATE OR REPLACE VIEW `{node.database}`.`{node.db_schema}`.`{node.alias}` AS (\n{sql_statement}\n)"
        if node.config.sql_header:
            sql_statement = f"{node.config.sql_header}\n{sql_statement}"
        return sql_statement

    def run(self, node: Node) -> DryRunResult:
        try:
            run_sql = insert_dependant_sql_literals(node, self._results)
        except UpstreamFailedException as e:
            return DryRunResult(node, None, DryRunStatus.FAILURE, 0, e)

        # Run the compiled code to get the total bytes processed
        compiled_sql = node.compiled_code
        if node.config.sql_header:
            compiled_sql = f"{node.config.sql_header}\n{compiled_sql}"
        _, _, total_bytes_processed, _ = self._sql_runner.query(compiled_sql)

        run_sql = self._modify_sql(node, run_sql)
        status, model_schema, _, exception = self._sql_runner.query(run_sql)

        result = DryRunResult(
            node, model_schema, status, total_bytes_processed, exception
        )
        return result
