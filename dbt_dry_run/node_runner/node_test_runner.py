from dbt_dry_run.exception import UpstreamFailedException
from dbt_dry_run.literals import insert_dependant_sql_literals
from dbt_dry_run.models.manifest import Node
from dbt_dry_run.node_runner import NodeRunner
from dbt_dry_run.results import DryRunResult, DryRunStatus


class NodeTestRunner(NodeRunner):
    resource_type = ("test",)

    def run(self, node: Node) -> DryRunResult:
        try:
            run_sql = insert_dependant_sql_literals(node, self._results)
        except UpstreamFailedException as e:
            return DryRunResult(node, None, DryRunStatus.FAILURE, e)

        status, predicted_table, exception = self._sql_runner.query(run_sql)

        result = DryRunResult(node, predicted_table, status, exception)
        return result
