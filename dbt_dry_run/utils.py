from typing import Dict
from dbt_dry_run.models import TableField, Table


def collect_field_dicts(
    fields: list[TableField], prefix: str = ""
) -> list[Dict[str, TableField]]:
    collected: list[Dict[str, TableField]] = []
    for field in fields:
        name = field.name
        field_type = field.type_
        path = f"{prefix}.{name}" if prefix else name
        collected.append({path: TableField(name=name, type=field_type)})
        if field.fields:
            collected.extend(collect_field_dicts(field.fields, path))
    return collected


def append_new_fields(
    dry_run_fields: list[TableField], target_fields: list[TableField]
) -> list[dict[str, TableField]]:
    dry_run_map = collect_field_dicts(dry_run_fields)
    target_map = collect_field_dicts(target_fields)

    target_field_paths = set(
        target_field_path
        for target_dict in target_map
        for target_field_path in target_dict.keys()
    )
    for dry_run_dict in dry_run_map:
        for field_path, field in dry_run_dict.items():
            if field_path not in target_field_paths:
                target_map.append(dry_run_dict)
    return target_map


def rebuild_nested_fields(new_target_map: list[dict[str, TableField]]) -> Table:
    final_fields = []
    for field in new_target_map:
        for key in field.keys():
            levels = key.split(".")
            if len(levels) == 1:
                final_fields.append(field.get(key))
            if levels[0] not in final_fields:
                final_fields.append(levels[0])
    return Table(fields=final_fields)
                # for level in levels[1:]:






## TODO 1 - Rebuild nested fields from path into schema
## TODO 2 - Update each schema change handler to use the new utils and test with nested fields
