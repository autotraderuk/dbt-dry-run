import os
from ast import literal_eval
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

import jinja2
import yaml
from pydantic import Field, root_validator
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

    @root_validator(pre=True)
    def validate_dataset_or_schema(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        if values.get("schema") and values.get("dataset"):
            raise ValueError(f"Must specify one of dataset or schema")
        elif not values.get("schema") and not values.get("dataset"):
            raise ValueError(f"Must specify dataset or schema")
        values["dataset"] = values.get("dataset", values.get("schema"))
        return values


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


def profiles_get_env_var(key: str, default: str = None) -> Optional[str]:
    return os.environ.get(key, default)


def as_number_filter(value: str) -> Any:
    return literal_eval(value)


def read_profiles(profile_string: str) -> Dict[str, Profile]:
    all_profiles: Dict[str, Profile] = {}

    template_loader = jinja2.DictLoader({"profiles.yml": profile_string})
    template_env = jinja2.Environment(loader=template_loader)
    template_env.globals.update(env_var=profiles_get_env_var)
    template_env.filters["as_number"] = as_number_filter
    template = template_env.get_template("profiles.yml")
    output_text = template.render()
    profile_data = yaml.safe_load(output_text)
    for name, profile in profile_data.items():
        if name != "config":
            all_profiles[name] = Profile(**profile)
    return all_profiles
