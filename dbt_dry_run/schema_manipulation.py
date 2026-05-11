from dbt_dry_run.models import TableField, Table
from copy import deepcopy
from dbt_dry_run.exception import SchemaChangeException

# BQ limitation
MAX_SUPPORTED_NESTED_FIELD_DEPTH = 15


def _get_new_fields(
    new_fields: list[TableField], existing_fields: list[TableField]
) -> list[TableField]:
    existing_field_paths = {field.path for field in existing_fields}
    fields_to_add: list[TableField] = []

    for field in new_fields:
        if field.path not in existing_field_paths:
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


def merge_table_fields(
    existing_fields: list[TableField], new_fields: list[TableField]
) -> list[TableField]:
    existing_fields_with_flattened_paths = Table.collect_flattened_field_paths(
        existing_fields, max_depth=MAX_SUPPORTED_NESTED_FIELD_DEPTH
    )
    new_fields_with_flattened_paths = Table.collect_flattened_field_paths(
        new_fields, max_depth=MAX_SUPPORTED_NESTED_FIELD_DEPTH
    )

    fields_to_add = _get_new_fields(
        new_fields_with_flattened_paths, existing_fields_with_flattened_paths
    )

    merged_table_fields = []
    ## Keep all existing fields, add any new nested fields to existing structs
    for existing_field in existing_fields:
        merged_struct_field = _add_fields_to_struct(existing_field, fields_to_add)
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


def assert_no_nested_fields_removed(
    new_fields: list[TableField], existing_fields: list[TableField]
) -> None:
    existing_fields_with_flattened_path = Table.collect_flattened_field_paths(
        existing_fields, max_depth=MAX_SUPPORTED_NESTED_FIELD_DEPTH
    )
    new_fields_with_flattened_path = Table.collect_flattened_field_paths(
        new_fields, max_depth=MAX_SUPPORTED_NESTED_FIELD_DEPTH
    )

    new_field_paths = set(
        new_field.path for new_field in new_fields_with_flattened_path
    )

    for field in existing_fields_with_flattened_path:
        if (
            field.path is not None
            and field.path not in new_field_paths
            and not field.is_top_level
        ):
            raise SchemaChangeException(
                f"Field '{'.'.join(field.path)}' has been removed from a nested column"
            )
