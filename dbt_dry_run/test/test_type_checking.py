from dbt_dry_run.models import BigQueryFieldType
from dbt_dry_run.type_checking import target_type_can_be_coerced_to_model_type


def test_type_mapping_for_allowed_types():
    target_type = BigQueryFieldType.STRING
    model_type = BigQueryFieldType.STRING

    assert target_type_can_be_coerced_to_model_type(target_type, model_type) == True

    target_type = BigQueryFieldType.INT64
    model_type = BigQueryFieldType.FLOAT64

    assert target_type_can_be_coerced_to_model_type(target_type, model_type) == True

    target_type = BigQueryFieldType.FLOAT64
    model_type = BigQueryFieldType.STRING

    assert target_type_can_be_coerced_to_model_type(target_type, model_type) == True


def test_type_mapping_fails_for_disallowed_types():
    target_type = BigQueryFieldType.FLOAT64
    model_type = BigQueryFieldType.INT64

    assert target_type_can_be_coerced_to_model_type(target_type, model_type) == False

    target_type = BigQueryFieldType.STRING
    model_type = BigQueryFieldType.FLOAT64

    assert target_type_can_be_coerced_to_model_type(target_type, model_type) == False