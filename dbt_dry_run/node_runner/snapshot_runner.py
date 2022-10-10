from typing import Set

from dbt_dry_run.exception import SnapshotConfigException, UpstreamFailedException
from dbt_dry_run.literals import insert_dependant_sql_literals
from dbt_dry_run.models import BigQueryFieldMode, BigQueryFieldType, Table, TableField
from dbt_dry_run.models.manifest import Node
from dbt_dry_run.node_runner import NodeRunner
from dbt_dry_run.results import DryRunResult, DryRunStatus


def _check_cols_missing(node: Node, table: Table) -> Set[str]:
    if not node.config.check_cols or node.config.check_cols == "all":
        return set()
    return set(filter(lambda col: col not in table.field_names, node.config.check_cols))


DBT_SNAPSHOT_FIELDS = [
    TableField(
        name="dbt_scd_id",
        type=BigQueryFieldType.STRING,
        mode=BigQueryFieldMode.NULLABLE,
    ),
    TableField(
        name="dbt_updated_at",
        type=BigQueryFieldType.TIMESTAMP,
        mode=BigQueryFieldMode.NULLABLE,
    ),
    TableField(
        name="dbt_valid_from",
        type=BigQueryFieldType.TIMESTAMP,
        mode=BigQueryFieldMode.NULLABLE,
    ),
    TableField(
        name="dbt_valid_to",
        type=BigQueryFieldType.TIMESTAMP,
        mode=BigQueryFieldMode.NULLABLE,
    ),
]


class SnapshotRunner(NodeRunner):
    resource_type = ("snapshot",)

    @staticmethod
    def _validate_snapshot_config(node: Node, result: DryRunResult) -> DryRunResult:
        if not result.table:
            raise ValueError("Can't validate result without table")
        if isinstance(node.config.unique_key, list):
            raise RuntimeError(
                f"Cannot dry run node '{node.unique_id}' because it is a snapshot"
                f" with a list of unique keys '{node.config.unique_key}'"
            )
        if node.config.unique_key not in result.table.field_names:
            exception = SnapshotConfigException(
                f"Missing `unique_key` column '{node.config.unique_key}'"
            )
            return DryRunResult(
                node=result.node,
                table=result.table,
                status=DryRunStatus.FAILURE,
                exception=exception,
            )
        if node.config.strategy == "timestamp":
            if node.config.updated_at not in result.table.field_names:
                exception = SnapshotConfigException(
                    f"Missing `updated_at` column '{node.config.updated_at}'"
                )
                return DryRunResult(
                    node=result.node,
                    table=result.table,
                    status=DryRunStatus.FAILURE,
                    exception=exception,
                )
        elif node.config.strategy == "check":
            if _check_cols_missing(node, result.table):
                exception = SnapshotConfigException(
                    f"Missing `check_cols` '{node.config.check_cols}'"
                )
                return DryRunResult(
                    node=result.node,
                    table=result.table,
                    status=DryRunStatus.FAILURE,
                    exception=exception,
                )
        else:
            raise ValueError(f"Unknown snapshot strategy: '{node.config.strategy}'")
        return result

    def run(self, node: Node) -> DryRunResult:
        try:
            run_sql = insert_dependant_sql_literals(node, self._results)
        except UpstreamFailedException as e:
            return DryRunResult(node, None, DryRunStatus.FAILURE, e)

        status, predicted_table, exception = self._sql_runner.query(run_sql)
        result = DryRunResult(node, predicted_table, status, exception)
        if result.status == DryRunStatus.SUCCESS and result.table:
            result.table.fields = [*result.table.fields, *DBT_SNAPSHOT_FIELDS]
            result = self._validate_snapshot_config(node, result)
        return result
