from typing import Callable, Dict, List, Set

from dbt_dry_run.models import Table, TableField
from dbt_dry_run.models.manifest import ManifestColumn, Node
from dbt_dry_run.results import DryRunResult, LintingError


def _extract_fields(table_fields: List[TableField], prefix: str = "") -> List[str]:
    field_names = []
    for field in table_fields:
        field_names.append(f"{prefix}{field.name}")
        if field.fields:
            new_prefix = f"{prefix}{field.name}."
            field_names.extend(_extract_fields(field.fields, prefix=new_prefix))
    return field_names


def expand_table_fields(table: Table) -> Set[str]:
    """
    Expand table fields to dot notation (like in dbt metadata)

    Eg: TableField(name="a", fields=[TableField(name="a1")])
    Returns: ["a", "a.a1"]
    """
    return set(_extract_fields(table.fields))


def get_extra_documented_columns(
    manifest: Dict[str, ManifestColumn], dry_run: Table
) -> List[str]:
    dry_run_column_names = expand_table_fields(dry_run)
    extra_columns_in_manifest = set(manifest.keys()) - set(dry_run_column_names)
    errors = []

    if extra_columns_in_manifest:
        for column in extra_columns_in_manifest:
            errors.append(f"Extra column in metadata: '{column}'")

    return errors


def get_undocumented_columns(
    manifest: Dict[str, ManifestColumn], dry_run: Table
) -> List[str]:
    dry_run_column_names = expand_table_fields(dry_run)
    missing_columns_in_manifest = set(dry_run_column_names) - set(manifest.keys())

    errors = []
    if missing_columns_in_manifest:
        for column in missing_columns_in_manifest:
            errors.append(f"Column not documented in metadata: '{column}'")

    return errors


RULES: Dict[str, Callable[[Dict[str, ManifestColumn], Table], List[str]]] = {
    "UNDOCUMENTED_COLUMNS": get_undocumented_columns,
    "EXTRA_DOCUMENTED_COLUMNS": get_extra_documented_columns,
}


def lint_columns(node: Node, result: DryRunResult) -> DryRunResult:
    if not result.table:
        return result

    all_errors = []

    for rule_name, rule_func in RULES.items():
        error_messages = rule_func(node.columns, result.table)
        errors = list(
            map(lambda err: LintingError(rule=rule_name, message=err), error_messages)
        )
        all_errors.extend(errors)
    return result.with_linting_errors(all_errors)
