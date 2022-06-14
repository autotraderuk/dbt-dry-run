from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import Field, root_validator
from pydantic.main import BaseModel


class BigQueryConnectionMethod(str, Enum):
    OAUTH = "oauth"
    SERVICE_ACCOUNT = "service-account"


class Output(BaseModel):
    output_type: str = Field(..., alias="type")
    method: BigQueryConnectionMethod
    project: str
    db_schema: str = Field(..., alias="schema")
    location: str
    threads: int = Field(..., ge=1)
    timeout_seconds: int = Field(..., ge=0)
    keyfile: Optional[Path] = None
    impersonate_service_account: Optional[str] = None
    scopes: List[str] = []


class Profile(BaseModel):
    outputs: Dict[str, Output]
    target: str

    @root_validator(pre=True)
    def target_must_be_valid_output(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        output_keys = set(values["outputs"].keys())
        target = values["target"]
        if target not in output_keys:
            raise ValueError(
                f"target={target} but it must be valid output={output_keys}"
            )
        return values
