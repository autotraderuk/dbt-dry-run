from dbt_dry_run.models import TableField
from dbt_dry_run.models.table import FieldLineage, Table
from copy import deepcopy

# BQ limitation
MAX_SUPPORTED_NESTED_FIELD_DEPTH = 15


def collect_field_lineages(
    fields: list[TableField], prefix: str = "", current_depth: int = 1
) -> list[FieldLineage]:
    collected: list[FieldLineage] = []
    for field in fields:
        name = field.name
        path = f"{prefix}.{name}" if prefix else name
        collected.append(FieldLineage(lineage=path, field=field))
        if field.fields and current_depth < MAX_SUPPORTED_NESTED_FIELD_DEPTH:
            collected.extend(
                collect_field_lineages(field.fields, path, current_depth + 1)
            )
    return collected


def find_missing_fields(
    dry_run_fields: list[TableField], target_fields: list[TableField]
) -> list[FieldLineage]:
    dry_run_fields_with_lineage = collect_field_lineages(dry_run_fields)
    target_fields_with_lineage = collect_field_lineages(target_fields)

    target_field_lineages = set(
        target_field.lineage for target_field in target_fields_with_lineage
    )
    missing_fields = []
    for dry_run_field in dry_run_fields_with_lineage:
        if dry_run_field.lineage not in target_field_lineages:
            missing_fields.append(
                FieldLineage(lineage=dry_run_field.lineage, field=dry_run_field.field)
            )
    return missing_fields


def add_missing_fields(
    target_field: TableField,
    missing_fields: list[FieldLineage],
    current_path: str = "",
    current_depth: int = 1,
) -> TableField:
    path = f"{current_path}.{target_field.name}" if current_path else target_field.name
    field_copy = deepcopy(target_field)

    # Recursively update child fields if they exist and we are below the nesting limit.
    if field_copy.fields and current_depth < MAX_SUPPORTED_NESTED_FIELD_DEPTH:
        child_fields = []
        for field in field_copy.fields:
            updated_child = add_missing_fields(
                field, missing_fields, path, current_depth + 1
            )
            child_fields.append(updated_child)
        field_copy.fields = child_fields
    else:
        field_copy.fields = field_copy.fields or None

    # Add missing fields whose parent lineage matches this field's path.
    for missing in missing_fields:
        parent_lineage = ".".join(missing.lineage.split(".")[:-1])
        if (
            parent_lineage == path
            and current_depth < MAX_SUPPORTED_NESTED_FIELD_DEPTH
            and len(missing.lineage.split(".")) <= MAX_SUPPORTED_NESTED_FIELD_DEPTH
        ):
            if field_copy.fields is None:
                field_copy.fields = []
            if not any(
                existing_field.name == missing.field.name
                for existing_field in field_copy.fields
            ):
                field_copy.fields.append(missing.field)
    return field_copy


def build_predicted_fields(
    target_table: Table,
    missing_fields: list[FieldLineage],
    included_top_level_field_names: set[str] | None = None,
) -> list[TableField]:
    predicted_fields = []
    for target_field in target_table.fields:
        if (
            included_top_level_field_names is not None
            and target_field.name not in included_top_level_field_names
        ):
            continue
        updated_field = add_missing_fields(target_field, missing_fields)
        predicted_fields.append(updated_field)

    # Add any missing top-level fields for sync_all_columns_handler
    top_level_missing = [
        missing_field
        for missing_field in missing_fields
        if "." not in missing_field.lineage
        and (
            included_top_level_field_names is None
            or missing_field.field.name in included_top_level_field_names
        )
    ]
    for missing_field in top_level_missing:
        if not any(f.name == missing_field.field.name for f in predicted_fields):
            predicted_fields.append(missing_field.field)
    return predicted_fields
