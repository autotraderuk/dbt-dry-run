from dbt_dry_run.models import BigQueryFieldType


def target_type_can_be_coerced_to_model_type(target_type: BigQueryFieldType, model_type: BigQueryFieldType) -> bool:
    compatible_target_types_by_model_type = {
        BigQueryFieldType.STRING: {
            BigQueryFieldType.STRING
        },
        BigQueryFieldType.INT64: {
            BigQueryFieldType.INT64,
            BigQueryFieldType.NUMERIC,
            BigQueryFieldType.BIGNUMERIC,
            BigQueryFieldType.FLOAT64,
        },
        BigQueryFieldType.FLOAT64: {
            BigQueryFieldType.INT64,
            BigQueryFieldType.NUMERIC,
            BigQueryFieldType.BIGNUMERIC,
            BigQueryFieldType.FLOAT64,
        },
    }

    if model_type in compatible_target_types_by_model_type[target_type]:
        return True
    else:
        return False