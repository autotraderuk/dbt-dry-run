from textwrap import dedent
from typing import Iterable

from dbt_dry_run.models.manifest import TableRef


def sql_has_recursive_ctes(code: str) -> bool:
    code_tokens = code.lower().split()
    for index in range(0, len(code_tokens) - 1):
        if code_tokens[index : index + 2] == ["with", "recursive"]:
            return True
    return False


def get_partition_columns_sql(table_ref: TableRef) -> str:
    project, dataset, table_name = table_ref.bq_literal.split(".")

    return dedent(
        f"""
        SELECT
            column_name
        FROM
            {project}.{dataset}.INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = "{table_name.replace("`","")}"
        AND is_partitioning_column = "YES"
        """
    )

def get_union_sql(
    table_ref: TableRef, common_field_names: Iterable[str], select_statement: str, partition_column_name: str
) -> str:
    values_csv = ",".join(sorted(common_field_names))

    select_statement_with_common_field_names = (
    f"""
    SELECT {values_csv} FROM ({select_statement})
    """
    )

    if partition_column_name:
        partition_column_filter = f"WHERE {partition_column_name} = (SELECT {partition_column_name} FROM ({select_statement_with_common_field_names}))"
    else:
        partition_column_filter = ""

    return dedent(
        f"""
        SELECT
            *
        FROM {table_ref.bq_literal}
        {partition_column_filter}
        UNION ALL (
          {select_statement_with_common_field_names}
        )
        """
    )
