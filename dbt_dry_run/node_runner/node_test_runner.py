from dbt_dry_run.exception import UpstreamFailedException
from dbt_dry_run.literals import insert_dependant_sql_literals
from dbt_dry_run.models.manifest import Node
from dbt_dry_run.node_runner import NodeRunner
from dbt_dry_run.results import DryRunResult, DryRunStatus


class NodeTestRunner(NodeRunner):
    def run(self, node: Node) -> DryRunResult:
        try:
            run_sql = insert_dependant_sql_literals(node, self._results)
        except UpstreamFailedException as e:
            return DryRunResult(node, None, DryRunStatus.FAILURE, 0, e)

        # Run the compiled code to get the total bytes processed
        _, _, total_bytes_processed, _ = self._sql_runner.query(node.compiled_code)
        (
            status,
            predicted_table,
            _,
            exception,
        ) = self._sql_runner.query(run_sql)

        result = DryRunResult(
            node, predicted_table, status, total_bytes_processed, exception
        )
        return result
