from typing import Dict, Set

from dbt_dry_run.models import BigQueryFieldType


class MergeTypeCompatibilityBlacklist:
    """
    Incompatible implicit conversions for merge assignment.

    Key is target table type, value contains model/output types that fail for implicit
    merge assignment even though UNION supertyping may still succeed.
    """

    _INCOMPATIBLE_MODEL_TYPES_BY_TARGET_TYPE: Dict[
        BigQueryFieldType, Set[BigQueryFieldType]
    ] = {
        BigQueryFieldType.INT64: {
            BigQueryFieldType.NUMERIC,
            BigQueryFieldType.BIGNUMERIC,
            BigQueryFieldType.FLOAT64,
        },
        BigQueryFieldType.NUMERIC: {
            BigQueryFieldType.BIGNUMERIC,
            BigQueryFieldType.FLOAT64,
        },
        BigQueryFieldType.BIGNUMERIC: {
            BigQueryFieldType.FLOAT64,
        },
        BigQueryFieldType.DATE: {
            BigQueryFieldType.DATETIME,
        },
        BigQueryFieldType.DATETIME: {
            BigQueryFieldType.DATE,
        },
    }

    @classmethod
    def is_incompatible(
        cls, target_type: BigQueryFieldType, model_type: BigQueryFieldType
    ) -> bool:
        return model_type in cls._INCOMPATIBLE_MODEL_TYPES_BY_TARGET_TYPE.get(
            target_type, set()
        )
