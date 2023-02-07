import os
from typing import List, Optional

import agate as ag
from agate import data_types

from dbt_dry_run.models import BigQueryFieldType, Table, TableField
from dbt_dry_run.models.manifest import Node
from dbt_dry_run.node_runner import NodeRunner
from dbt_dry_run.results import DryRunResult, DryRunStatus


class SeedRunner(NodeRunner):
    resource_type = ("seed",)

    DEFAULT_TYPE = BigQueryFieldType.STRING
    TYPE_MAP = {
        data_types.Text: BigQueryFieldType.STRING,
        data_types.Number: BigQueryFieldType.FLOAT64,
        data_types.Boolean: BigQueryFieldType.BOOLEAN,
        data_types.Date: BigQueryFieldType.DATE,
        data_types.DateTime: BigQueryFieldType.DATETIME,
    }

    def run(self, node: Node) -> DryRunResult:
        if not node.root_path:
            raise ValueError(f"Node {node.unique_id} does not have `root_path`")
        full_path = os.path.join(node.root_path, node.original_file_path)
        with open(full_path, "r", encoding="utf-8-sig") as f:
            csv_table = ag.Table.from_csv(f)

        fields: List[TableField] = []
        for column in csv_table.columns:
            type_ = self.TYPE_MAP.get(column.data_type.__class__, self.DEFAULT_TYPE)
            new_field = TableField(name=column.name, type=type_)
            fields.append(new_field)

        schema = Table(fields=fields)
        return DryRunResult(
            node=node, table=schema, status=DryRunStatus.SUCCESS, exception=None
        )

    def validate_node(self, node: Node) -> Optional[DryRunResult]:
        return None
