from dbt_dry_run.exception import UpstreamFailedException
from dbt_dry_run.models.dry_run_result import DryRunResult
from dbt_dry_run.models.manifest import Node
from dbt_dry_run.models.report import DryRunStatus
from dbt_dry_run.node_runner import NodeRunner
from dbt_dry_run.sql.statements import SQLPreprocessor, insert_dependant_sql_literals


class NodeTestRunner(NodeRunner):
    preprocessor = SQLPreprocessor([insert_dependant_sql_literals])

    def run(self, node: Node) -> DryRunResult:
        try:
            run_sql = self.preprocessor(node, self._results)
        except UpstreamFailedException as e:
            return DryRunResult(node, None, DryRunStatus.FAILURE, e)
        (
            status,
            predicted_table,
            exception,
        ) = self._sql_runner.query(run_sql)

        result = DryRunResult(node, predicted_table, status, exception)
        return result
