from typing import Dict
from dbt_dry_run.models import TableField

def collect_field_dicts(fields: list[TableField], prefix: str = "") -> list[Dict[str, TableField]]:
    collected: list[Dict[str, TableField]] = []
    for field in fields:
        name = field.name
        path = f"{prefix}.{name}" if prefix else name
        collected.append({path: field})
        if field.fields:
            collected.extend(collect_field_dicts(field.fields, path))
    return collected

def find_missing_fields(dry_run_fields: list[TableField], target_fields: list[TableField]):
    dry_run_map = collect_field_dicts(dry_run_fields)
    target_map = collect_field_dicts(target_fields)

    target_field_paths = set(target_field_path for target_dict in target_map for target_field_path in target_dict.keys())
    missing_fields = []
    for dry_run_dict in dry_run_map:
        for field_path, field in dry_run_dict.items():
            if field_path not in target_field_paths:
                missing_fields.append(dry_run_dict)
    return missing_fields

## TODO 1 - Append missing fields to target table fields
## TODO 2 - Rebuild nested fields from path into schema
## TODO 3 - Update each schema change handler to use the new utils and test with nested fields