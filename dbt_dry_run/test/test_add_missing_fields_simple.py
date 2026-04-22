import pytest
from dbt_dry_run.models import TableField
from dbt_dry_run.models.table import FieldLineage, Table
from dbt_dry_run.utils import add_missing_fields

class DummyFieldType:
    STRING = "STRING"
    STRUCT = "STRUCT"
    NUMERIC = "NUMERIC"


def test_add_missing_fields_simple():
    # Existing field: struct_field (STRUCT) with one child lv2 (STRING)
    struct_field = TableField(
        name="struct_field",
        type_=DummyFieldType.STRUCT,
        fields=[
            TableField(name="lv2", type_=DummyFieldType.STRING, fields=None)
        ],
    )
    # Missing field: struct_field.lv2.lv3 (NUMERIC)
    missing_lv3 = TableField(name="lv3", type_=DummyFieldType.NUMERIC, fields=None)
    missing_lineage = FieldLineage(lineage="struct_field.lv2.lv3", field=missing_lv3)

    # Add missing field
    result = add_missing_fields(struct_field, [missing_lineage])

    # Check that lv3 is now a child of lv2
    assert result.fields[0].fields is not None
    assert len(result.fields[0].fields) == 1
    assert result.fields[0].fields[0].name == "lv3"
    assert result.fields[0].fields[0].type_ == DummyFieldType.NUMERIC
