from textwrap import dedent
from typing import Iterable

from dbt_dry_run.models.manifest import TableRef


def sql_has_recursive_ctes(code: str) -> bool:
    code_tokens = code.lower().split()
    for index in range(0, len(code_tokens) - 1):
        if code_tokens[index : index + 2] == ["with", "recursive"]:
            return True
    return False


def get_union_sql(
    table_ref: TableRef, common_field_names: Iterable[str], select_statement: str
) -> str:
    values_csv = ",".join(sorted(common_field_names))
    select_statement_with_common_field_names = (
    f"""
    SELECT {values_csv} FROM ({select_statement})
    """
    )
    return dedent(
        f"""
        SELECT
            *
        FROM {table_ref.bq_literal}
        UNION ALL (
          {select_statement_with_common_field_names}
        )
        """
    )
