from dbt_dry_run.models import TableField
from dbt_dry_run.models.table import FieldPath, Table
from copy import deepcopy
from dbt_dry_run.exception import SchemaChangeException

# BQ limitation
MAX_SUPPORTED_NESTED_FIELD_DEPTH = 15


def collect_field_paths_for_table(
    fields: list[TableField], prefix: tuple[str, ...] = (), current_depth: int = 1
) -> list[FieldPath]:
    collected: list[FieldPath] = []
    for field in fields:
        name = field.name
        path = prefix + (name,)
        collected.append(FieldPath(path=path, field=field))
        if field.fields and current_depth < MAX_SUPPORTED_NESTED_FIELD_DEPTH:
            collected.extend(
                collect_field_paths_for_table(field.fields, path, current_depth + 1)
            )
    return collected


def get_model_fields_not_present_in_target(
    model_fields: list[TableField], target_fields: list[TableField]
) -> list[FieldPath]:
    model_fields_with_paths = collect_field_paths_for_table(model_fields)
    target_fields_with_paths = collect_field_paths_for_table(target_fields)

    target_field_paths = set(
        target_field.path for target_field in target_fields_with_paths
    )

    fields_unique_to_model = []

    for model_field in model_fields_with_paths:
        if model_field.path not in target_field_paths:
            fields_unique_to_model.append(
                FieldPath(path=model_field.path, field=model_field.field)
            )
    return fields_unique_to_model


def assert_model_removes_no_nested_fields_from_target(
    model_fields: list[TableField], target_fields: list[TableField]
) -> None:
    model_fields_with_paths = collect_field_paths_for_table(model_fields)
    target_fields_with_paths = collect_field_paths_for_table(target_fields)

    target_field_paths = set(
        target_field.path for target_field in target_fields_with_paths
    )

    model_field_paths = set(model_field.path for model_field in model_fields_with_paths)

    for target_field in target_field_paths:
        if target_field not in model_field_paths and len(target_field) > 1:
            raise SchemaChangeException(
                f"Field '{'.'.join(target_field)}' has been removed from a nested column"
            )


def add_field_paths_to_struct(
    struct: TableField,
    field_paths: list[FieldPath],
    current_path: tuple[str, ...] = (),
    current_depth: int = 1,
) -> TableField:
    path = current_path + (struct.name,)
    field_copy = deepcopy(struct)

    # Recursively update child fields if they exist and we are below the nesting limit.
    if field_copy.fields and current_depth < MAX_SUPPORTED_NESTED_FIELD_DEPTH:
        child_fields = []
        for field in field_copy.fields:
            updated_child = add_field_paths_to_struct(
                field, field_paths, path, current_depth + 1
            )
            child_fields.append(updated_child)
        field_copy.fields = child_fields
    else:
        field_copy.fields = field_copy.fields or None

    # Add missing fields whose parent path matches this field's path.
    for missing_field in field_paths:
        parent_path = missing_field.path[:-1]
        if (
            parent_path == path
            and current_depth < MAX_SUPPORTED_NESTED_FIELD_DEPTH
            and len(missing_field.path) <= MAX_SUPPORTED_NESTED_FIELD_DEPTH
        ):
            if field_copy.fields is None:
                field_copy.fields = []
            if not any(
                existing_field.name == missing_field.field.name
                for existing_field in field_copy.fields
            ):
                field_copy.fields.append(missing_field.field)
    return field_copy


def add_new_fields_to_table(
    table: Table, new_fields: list[FieldPath]
) -> list[TableField]:
    updated_schema = []
    for table_field in table.fields:
        updated_struct_field = add_field_paths_to_struct(table_field, new_fields)
        updated_schema.append(updated_struct_field)

    # Add any new top-level fields
    top_level_new_field = [
        new_field for new_field in new_fields if new_field.is_top_level
    ]

    for new_field in top_level_new_field:
        if not any(f.name == new_field.field.name for f in updated_schema):
            updated_schema.append(new_field.field)
    return updated_schema
