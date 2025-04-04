from dbt_dry_run.exception import UpstreamFailedException
from dbt_dry_run.models.manifest import Node
from dbt_dry_run.node_runner import NodeRunner
from dbt_dry_run.results import DryRunResult, DryRunStatus
from dbt_dry_run.sql.literals import insert_dependant_sql_literals


class TableRunner(NodeRunner):
    def _modify_sql(self, node: Node, sql_statement: str) -> str:
        if node.config.sql_header:
            sql_statement = f"{node.config.sql_header}\n{sql_statement}"
        return sql_statement

    def run(self, node: Node) -> DryRunResult:
        try:
            run_sql = insert_dependant_sql_literals(node, self._results)
        except UpstreamFailedException as e:
            return DryRunResult(node, None, DryRunStatus.FAILURE, 0, e)

        run_sql = self._modify_sql(node, run_sql)
        status, model_schema, total_bytes_processed, exception = self._sql_runner.query(
            run_sql
        )

        result = DryRunResult(
            node, model_schema, status, total_bytes_processed, exception
        )
        return result
