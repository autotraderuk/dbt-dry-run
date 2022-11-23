from itertools import groupby
from typing import Dict, List, Set, Tuple

from dbt_dry_run.exception import InvalidColumnSpecification, UnknownDataTypeException
from dbt_dry_run.models import BigQueryFieldMode, BigQueryFieldType, Table, TableField
from dbt_dry_run.models.manifest import ManifestColumn

REPEATED_SUFFIX = "[]"
STRUCT_SEPERATOR = "."
STRUCT_SEPERATOR_LENGTH = len(STRUCT_SEPERATOR)


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


def _column_is_repeated(data_type: str) -> bool:
    return data_type.endswith(REPEATED_SUFFIX)


def _split_column_data_type_and_mode(
    data_type: str,
) -> Tuple[BigQueryFieldType, BigQueryFieldMode]:
    mode = (
        BigQueryFieldMode.REPEATED
        if _column_is_repeated(data_type)
        else BigQueryFieldMode.NULLABLE
    )
    if mode == BigQueryFieldMode.REPEATED:
        clean_data_type = data_type[: -len(REPEATED_SUFFIX)]
    else:
        clean_data_type = data_type

    try:
        return BigQueryFieldType(clean_data_type), mode
    except ValueError:
        raise UnknownDataTypeException(
            f"Could not parse data_type `{clean_data_type}` from manifest"
        )


def _get_sub_field_map(
    cols: Dict[str, ManifestColumn], sub_field_columns: List[str], root_name: str
) -> Dict[str, ManifestColumn]:
    root_prefix_size = len(root_name) + STRUCT_SEPERATOR_LENGTH
    sub_field_names = list(
        map(
            lambda col: col[root_prefix_size:],
            sub_field_columns[1:],
        )
    )
    sub_field_map = {
        col: cols[root_name + STRUCT_SEPERATOR + col] for col in sub_field_names
    }
    return sub_field_map


def _to_fields(cols: Dict[str, ManifestColumn]) -> List[TableField]:
    if not cols:
        raise InvalidColumnSpecification(
            "Schema not specified in `columns` attribute in metadata"
        )
    sorted_columns = sorted(cols.keys())
    grouped_columns = groupby(
        sorted_columns, lambda val: val.split(STRUCT_SEPERATOR)[0]
    )
    fields = {}
    for root_name, group_cols_iterator in grouped_columns:
        group_cols = list(group_cols_iterator)
        if group_cols[0] != root_name:
            raise InvalidColumnSpecification(
                f"Could not find root record '{root_name}' for struct fields in metadata '{group_cols}'"
            )
        column = cols[root_name]
        if not column.data_type:
            raise UnknownDataTypeException(
                f"Can't determine schema of column '{root_name}' without 'data_type' in metadata"
            )
        sub_field_map = _get_sub_field_map(cols, group_cols, root_name)
        if sub_field_map:
            sub_fields = _to_fields(sub_field_map)
        else:
            sub_fields = None
        field_data_type, field_mode = _split_column_data_type_and_mode(column.data_type)
        fields[root_name] = TableField(
            name=root_name, type=field_data_type, mode=field_mode, fields=sub_fields
        )

    return list(fields.values())


def map_columns_to_table(columns: Dict[str, ManifestColumn]) -> Table:
    return Table(fields=_to_fields(columns))
