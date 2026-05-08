from dbt_dry_run.models import TableField
from dbt_dry_run.models.table import FieldPath
from copy import deepcopy
from dbt_dry_run.exception import SchemaChangeException

# BQ limitation
MAX_SUPPORTED_NESTED_FIELD_DEPTH = 15


def _collect_flattened_field_paths(
    fields: list[TableField], prefix: tuple[str, ...] = (), current_depth: int = 1
) -> list[FieldPath]:
    collected: list[FieldPath] = []
    for field in fields:
        name = field.name
        path = prefix + (name,)
        collected.append(FieldPath(path=path, field=field))
        if field.fields and current_depth < MAX_SUPPORTED_NESTED_FIELD_DEPTH:
            collected.extend(
                _collect_flattened_field_paths(field.fields, path, current_depth + 1)
            )
    return collected


def _get_new_fields(
    new_fields: list[FieldPath], existing_fields: list[FieldPath]
) -> list[FieldPath]:
    existing_field_paths = {field.path for field in existing_fields}
    fields_to_add = []

    for field in new_fields:
        if field.path not in existing_field_paths:
            fields_to_add.append(FieldPath(path=field.path, field=field.field))
    return fields_to_add


def _add_fields_to_struct(
    struct: TableField,
    nested_fields: list[FieldPath],
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
        parent_path = new_field.path[:-1]
        if (
            parent_path == path
            and current_depth < MAX_SUPPORTED_NESTED_FIELD_DEPTH
            and len(new_field.path) <= MAX_SUPPORTED_NESTED_FIELD_DEPTH
        ):
            if field_copy.fields is None:
                field_copy.fields = []
            if not any(
                existing_field.name == new_field.field.name
                for existing_field in field_copy.fields
            ):
                field_copy.fields.append(new_field.field)
    return field_copy


def merge_table_fields(
    existing_fields: list[TableField], new_fields: list[TableField]
) -> list[TableField]:
    existing_fields_with_flattened_paths = _collect_flattened_field_paths(
        existing_fields
    )
    new_fields_with_flattened_paths = _collect_flattened_field_paths(new_fields)

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
        if not any(f.name == new_field.field.name for f in merged_table_fields):
            merged_table_fields.append(new_field.field)

    return merged_table_fields


def assert_no_nested_fields_removed(
    new_fields: list[TableField], existing_fields: list[TableField]
) -> None:
    existing_fields_with_flattened_path = _collect_flattened_field_paths(
        existing_fields
    )
    new_fields_with_flattened_path = _collect_flattened_field_paths(new_fields)

    new_field_paths = set(
        new_field.path for new_field in new_fields_with_flattened_path
    )

    for field in existing_fields_with_flattened_path:
        if field.path not in new_field_paths and not field.is_top_level:
            raise SchemaChangeException(
                f"Field '{'.'.join(field.path)}' has been removed from a nested column"
            )
