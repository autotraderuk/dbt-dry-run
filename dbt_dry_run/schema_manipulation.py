from dbt_dry_run.models import TableField
from dbt_dry_run.models.table import FieldPath, Table
from copy import deepcopy
from dbt_dry_run.exception import SchemaChangeException

# BQ limitation
MAX_SUPPORTED_NESTED_FIELD_DEPTH = 15


def _collect_field_paths_for_table(
    fields: list[TableField], prefix: tuple[str, ...] = (), current_depth: int = 1
) -> list[FieldPath]:
    collected: list[FieldPath] = []
    for field in fields:
        name = field.name
        path = prefix + (name,)
        collected.append(FieldPath(path=path, field=field))
        if field.fields and current_depth < MAX_SUPPORTED_NESTED_FIELD_DEPTH:
            collected.extend(
                _collect_field_paths_for_table(field.fields, path, current_depth + 1)
            )
    return collected


def _get_fields_not_present_in_table(
    new_field_paths: list[FieldPath], table_field_paths: list[FieldPath]
) -> list[FieldPath]:
    fields_not_present_in_table = []

    for new_field_path in new_field_paths:
        if new_field_path.path not in table_field_paths:
            fields_not_present_in_table.append(
                FieldPath(path=new_field_path.path, field=new_field_path.field)
            )
    return fields_not_present_in_table


def _assert_no_nested_fields_removed_from_table(
    new_field_paths: list[FieldPath], existing_table_field_paths: list[FieldPath]
) -> None:
    table_field_paths = set(
        table_field.path for table_field in existing_table_field_paths
    )

    new_field_path_set = set(new_field.path for new_field in new_field_paths)

    for table_field in table_field_paths:
        if table_field not in new_field_path_set and len(table_field) > 1:
            raise SchemaChangeException(
                f"Field '{'.'.join(table_field)}' has been removed from a nested column"
            )


def _add_field_paths_to_struct(
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
            updated_child = _add_field_paths_to_struct(
                field, field_paths, path, current_depth + 1
            )
            child_fields.append(updated_child)
        field_copy.fields = child_fields
    else:
        field_copy.fields = field_copy.fields or None

    # Add new nested fields whose parent path matches this field's path.
    for new_field in field_paths:
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
    table_fields_1: list[TableField], table_fields_2: list[TableField]
) -> list[TableField]:

    field_paths_1 = _collect_field_paths_for_table(table_fields_1)
    field_paths_2 = _collect_field_paths_for_table(table_fields_2)

    new_fields = _get_fields_not_present_in_table(
        field_paths_2, field_paths_1
    )

    # _assert_no_nested_fields_removed_from_table(
    #     table_fields_1_with_paths, table_fields_2_with_paths
    # )

    merged_table_fields = []
    for table_field in table_fields_1:
        merged_struct_field = _add_field_paths_to_struct(table_field, new_fields)
        merged_table_fields.append(merged_struct_field)

    # Add any new top-level fields
    top_level_new_field = [
        new_field for new_field in new_fields if new_field.is_top_level
    ]

    for new_field in top_level_new_field:
        if not any(f.name == new_field.field.name for f in merged_table_fields):
            merged_table_fields.append(new_field.field)
    return merged_table_fields
