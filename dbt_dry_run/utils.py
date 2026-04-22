from dbt_dry_run.models import TableField
from dbt_dry_run.models.table import FieldLineage


def collect_field_lineages(
    fields: list[TableField], prefix: str = ""
) -> list[FieldLineage]:
    collected: list[FieldLineage] = []
    for field in fields:
        name = field.name
        path = f"{prefix}.{name}" if prefix else name
        collected.append(FieldLineage(lineage=path, field=field))
        if field.fields:
            collected.extend(collect_field_lineages(field.fields, path))
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


# def rebuild_nested_fields(new_target_map: list[dict[str, TableField]]) -> Table:
#     final_fields = []
#     for field in new_target_map:
#         for key in field.keys():
#             levels = key.split(".")
#             if len(levels) == 1:
#                 final_fields.append(field.get(key))
#             if levels[0] not in final_fields:
#                 final_fields.append(levels[0])
#     return Table(fields=final_fields)
# for level in levels[1:]:


## TODO 1 - Rebuild nested fields from path into schema
## TODO 2 - Update each schema change handler to use the new utils and test with nested fields
