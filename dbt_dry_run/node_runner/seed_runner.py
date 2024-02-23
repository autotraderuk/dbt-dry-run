import os
from typing import List, Optional

import agate as ag

from dbt_dry_run.exception import UnknownSchemaException
from dbt_dry_run.models import BigQueryFieldType, Table, TableField
from dbt_dry_run.models.manifest import Node
from dbt_dry_run.node_runner import NodeRunner
from dbt_dry_run.results import DryRunResult, DryRunStatus


class SeedRunner(NodeRunner):
    def run(self, node: Node) -> DryRunResult:
        if not node.root_path:
            raise ValueError(f"Node {node.unique_id} does not have `root_path`")
        full_path = os.path.join(node.root_path, node.original_file_path)
        with open(full_path, "r", encoding="utf-8-sig") as f:
            csv_table = ag.Table.from_csv(f)

        fields: List[TableField] = []
        for idx, column in enumerate(csv_table.columns):
            override_type = node.config.column_types.get(column.name)
            new_type = override_type or self._sql_runner.convert_agate_type(
                csv_table, idx
            )
            if new_type is None:
                msg = f"Unknown Big Query schema for seed '{node.unique_id}' Column '{column.name}'"
                exception = UnknownSchemaException(msg)
                return DryRunResult(
                    node=node,
                    table=None,
                    status=DryRunStatus.FAILURE,
                    exception=exception,
                )
            new_field = TableField(
                name=column.name, type=BigQueryFieldType[new_type.upper()]
            )
            fields.append(new_field)

        schema = Table(fields=fields)
        return DryRunResult(
            node=node, table=schema, status=DryRunStatus.SUCCESS, exception=None
        )

    def validate_node(self, node: Node) -> Optional[DryRunResult]:
        return None
