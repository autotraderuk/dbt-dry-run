from enum import Enum
from pathlib import Path
from typing import Any, List, Literal, Optional

from pydantic import BaseModel, Field, model_validator


class BigQueryConnectionMethod(str, Enum):
    OAUTH = "oauth"
    SERVICE_ACCOUNT = "service-account"
    EXTERNAL_OAUTH_WIF = "external-oauth-wif"


class TokenEndpoint(BaseModel):
    type: Literal["entra"]
    request_url: str
    request_data: str = Field(repr=False)


class Output(BaseModel):
    output_type: Literal["bigquery"] = Field(..., alias="type")
    method: BigQueryConnectionMethod
    project: Optional[str] = None
    dataset: str
    location: Optional[str] = None
    threads: int = Field(default=4, ge=1)
    execution_project: Optional[str] = None
    quota_project: Optional[str] = None
    api_endpoint: Optional[str] = None
    priority: Optional[Literal["interactive", "batch"]] = None
    maximum_bytes_billed: Optional[int] = Field(default=None, ge=0)
    reservation: Optional[str] = None
    job_creation_timeout_seconds: Optional[int] = Field(default=None, ge=0)
    job_execution_timeout_seconds: Optional[int] = Field(default=None, ge=0)
    keyfile: Optional[Path] = None
    workload_pool_provider_path: Optional[str] = None
    token_endpoint: Optional[TokenEndpoint] = Field(default=None, repr=False)
    service_account_impersonation_url: Optional[str] = None
    scopes: List[str] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def normalize_aliases(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            return values
        values = values.copy()
        for old, new in (
            ("database", "project"),
            ("schema", "dataset"),
            ("timeout_seconds", "job_execution_timeout_seconds"),
        ):
            if old in values:
                if new in values and values[new] != values[old]:
                    raise ValueError(f"Conflicting profile fields: {old} and {new}")
                values[new] = values.pop(old)
        return values

    @model_validator(mode="after")
    def validate_auth(self) -> "Output":
        if self.method == BigQueryConnectionMethod.SERVICE_ACCOUNT and not self.keyfile:
            raise ValueError("service-account requires keyfile")
        if self.method == BigQueryConnectionMethod.EXTERNAL_OAUTH_WIF and (
            not self.workload_pool_provider_path or not self.token_endpoint
        ):
            raise ValueError(
                "external-oauth-wif requires workload_pool_provider_path and token_endpoint"
            )
        return self
