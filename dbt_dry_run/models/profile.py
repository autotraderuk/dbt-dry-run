from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import Field, model_validator
from pydantic.main import BaseModel


class BigQueryConnectionMethod(str, Enum):
    OAUTH = "oauth"
    SERVICE_ACCOUNT = "service-account"


class Output(BaseModel):
    output_type: str = Field(..., alias="type")
    method: BigQueryConnectionMethod
    project: str
    dataset: Optional[str] = None
    location: str
    threads: int = Field(..., ge=1)
    timeout_seconds: int = Field(..., ge=0)
    keyfile: Optional[Path] = None
    impersonate_service_account: Optional[str] = None
    scopes: List[str] = []

    @model_validator(mode="before")
    def validate_dataset_or_schema(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        if values.get("schema") and values.get("dataset"):
            raise ValueError("Must specify one of dataset or schema")
        elif not values.get("schema") and not values.get("dataset"):
            raise ValueError("Must specify dataset or schema")
        values["dataset"] = values.get("dataset", values.get("schema"))
        return values


class Profile(BaseModel):
    outputs: Dict[str, Output]
    target: str

    @model_validator(mode="before")
    def target_must_be_valid_output(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        output_keys = set(values["outputs"].keys())
        target = values["target"]
        if target not in output_keys:
            raise ValueError(
                f"target={target} but it must be valid output={output_keys}"
            )
        return values
