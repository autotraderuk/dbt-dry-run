from dbt_dry_run.exception import UpstreamFailedException
from dbt_dry_run.models.manifest import Node
from dbt_dry_run.node_runner import NodeRunner
from dbt_dry_run.results import DryRunResult, DryRunStatus
from dbt_dry_run.sql.statements import (
    SQLPreprocessor,
    add_sql_header,
    insert_dependant_sql_literals,
)


class TableRunner(NodeRunner):
    preprocessor = SQLPreprocessor([insert_dependant_sql_literals, add_sql_header])

    def run(self, node: Node) -> DryRunResult:
        try:
            run_sql = self.preprocessor(node, self._results)
        except UpstreamFailedException as e:
            return DryRunResult(node, None, DryRunStatus.FAILURE, e)
        status, model_schema, exception = self._sql_runner.query(run_sql)

        result = DryRunResult(node, model_schema, status, exception)
        return result
