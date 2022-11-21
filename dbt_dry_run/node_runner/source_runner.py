from typing import Optional

from dbt_dry_run.columns_metadata import map_columns_to_table
from dbt_dry_run.exception import SourceMissingException
from dbt_dry_run.models import Table
from dbt_dry_run.models.manifest import Node
from dbt_dry_run.node_runner import NodeRunner
from dbt_dry_run.results import DryRunResult, DryRunStatus


class SourceRunner(NodeRunner):
    resource_type = ("source",)

    def run(self, node: Node) -> DryRunResult:
        exception: Optional[Exception] = None
        predicted_table: Optional[Table] = None
        status = DryRunStatus.SUCCESS
        if node.is_external_source():
            try:
                predicted_table = map_columns_to_table(node.columns)
            except ValueError as e:
                status = DryRunStatus.FAILURE
                exception = e
        else:
            if not self._sql_runner.node_exists(node):
                status = DryRunStatus.FAILURE
                exception = SourceMissingException(
                    f"Could not find source in target environment for node '{node.unique_id}'"
                )

        return DryRunResult(node, predicted_table, status, exception)
