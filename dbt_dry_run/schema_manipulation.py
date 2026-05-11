from dbt_dry_run.models import TableField, Table
from copy import deepcopy

from dbt_dry_run.models.table import MAX_SUPPORTED_NESTED_FIELD_DEPTH


def merge_table_fields(
    table_1_fields: list[TableField], table_2_fields: list[TableField]
) -> list[TableField]:
    table_1_fields_with_flattened_paths = Table.collect_flattened_field_paths(
        table_1_fields, max_depth=MAX_SUPPORTED_NESTED_FIELD_DEPTH
    )

    table_2_fields_with_flattened_paths = Table.collect_flattened_field_paths(
        table_2_fields, max_depth=MAX_SUPPORTED_NESTED_FIELD_DEPTH
    )

    fields_to_add = _get_fields_in_table_1_that_are_not_in_table_2(
        table_1_fields_with_flattened_paths, table_2_fields_with_flattened_paths
    )

    merged_table_fields = []
    ## Keep all existing fields, add any new nested fields to existing structs
    for table_2_field in table_2_fields:
        merged_struct_field = _add_fields_to_struct(table_2_field, fields_to_add)
        merged_table_fields.append(merged_struct_field)

    # Add any new top-level fields
    top_level_new_field = [
        new_field for new_field in fields_to_add if new_field.is_top_level
    ]
    for new_field in top_level_new_field:
        if not any(f.name == new_field.name for f in merged_table_fields):
            merged_table_fields.append(
                new_field.model_copy(deep=True, update={"path": None})
            )

    return merged_table_fields


def _get_fields_in_table_1_that_are_not_in_table_2(
    table_1_fields: list[TableField], table_2_fields: list[TableField]
) -> list[TableField]:
    table_2_paths = {field.path for field in table_2_fields}
    fields_to_add: list[TableField] = []

    for field in table_1_fields:
        if field.path not in table_2_paths:
            fields_to_add.append(field)
    return fields_to_add


def _add_fields_to_struct(
    struct: TableField,
    nested_fields: list[TableField],
    current_path: tuple[str, ...] = (),
    current_depth: int = 1,
) -> TableField:
    path = current_path + (struct.name,)
    field_copy = deepcopy(struct)

    # Recursively update child fields if they exist and we are below the nesting limit.
    if field_copy.fields and current_depth < MAX_SUPPORTED_NESTED_FIELD_DEPTH:
        child_fields = []
        for field in field_copy.fields:
            updated_child = _add_fields_to_struct(
                field, nested_fields, path, current_depth + 1
            )
            child_fields.append(updated_child)
        field_copy.fields = child_fields
    else:
        field_copy.fields = field_copy.fields or None

    # Add new nested fields whose parent path matches this field's path.
    for new_field in nested_fields:
        if new_field.path is None:
            continue
        parent_path = new_field.path[:-1]
        if (
            parent_path == path
            and current_depth < MAX_SUPPORTED_NESTED_FIELD_DEPTH
            and len(new_field.path) <= MAX_SUPPORTED_NESTED_FIELD_DEPTH
        ):
            if field_copy.fields is None:
                field_copy.fields = []
            if not any(
                existing_field.name == new_field.name
                for existing_field in field_copy.fields
            ):
                field_copy.fields.append(
                    new_field.model_copy(deep=True, update={"path": None})
                )
    return field_copy
