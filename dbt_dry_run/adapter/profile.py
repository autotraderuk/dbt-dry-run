import os
from ast import literal_eval
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

import jinja2
from jinja2 import nodes
from pydantic import ValidationError
import yaml

from dbt_dry_run.models.profile import Output


_MISSING = object()


def as_number_filter(value: Any) -> Any:
    number = literal_eval(str(value))
    if isinstance(number, bool) or not isinstance(number, (int, float)):
        raise ValueError("Expected a number in profile template")
    return number


def as_bool_filter(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if str(value).lower() in ("true", "false"):
        return str(value).lower() == "true"
    raise ValueError("Expected a boolean in profile template")


def as_native_filter(value: Any) -> Any:
    return yaml.safe_load(value) if isinstance(value, str) else value


class ProfileRenderer:
    """Render YAML scalar values with the dbt profiles.yml Jinja context."""

    def __init__(self, cli_vars: Optional[Mapping[str, Any]] = None):
        self._vars = cli_vars or {}
        self._secret_accessed = False
        self._environment = jinja2.Environment(undefined=jinja2.StrictUndefined)
        self._environment.globals.update(env_var=self._env_var, var=self._var)
        self._environment.filters.update(
            as_number=as_number_filter,
            as_bool=as_bool_filter,
            as_native=as_native_filter,
            as_text=str,
        )

    def _env_var(self, key: str, default: Any = _MISSING) -> Any:
        if key.startswith("DBT_ENV_SECRET_"):
            self._secret_accessed = True
        if key in os.environ:
            return os.environ[key]
        if default is not _MISSING:
            return default
        raise ValueError(f"Required environment variable {key!r} is not set")

    def _var(self, key: str, default: Any = _MISSING) -> Any:
        if key in self._vars:
            return self._vars[key]
        if default is not _MISSING:
            return default
        raise ValueError(f"Required CLI variable {key!r} is not set")

    @staticmethod
    def _direct_env_var_call(parsed: nodes.Template) -> bool:
        if len(parsed.body) != 1 or not isinstance(parsed.body[0], nodes.Output):
            return False
        children = parsed.body[0].nodes
        return (
            len(children) == 1
            and isinstance(children[0], nodes.Call)
            and isinstance(children[0].node, nodes.Name)
            and children[0].node.name == "env_var"
        )

    def render_scalar(self, value: Any) -> Any:
        if not isinstance(value, str) or "{%" not in value and "{{" not in value:
            return value
        self._secret_accessed = False
        parsed = self._environment.parse(value)
        direct_env_var = self._direct_env_var_call(parsed)
        try:
            # A single expression retains its Python type. Other Jinja templates
            # (including control blocks) render to text, as in dbt YAML fields.
            if len(parsed.body) == 1 and isinstance(parsed.body[0], nodes.Output):
                children = parsed.body[0].nodes
                if len(children) == 1 and not isinstance(
                    children[0], nodes.TemplateData
                ):
                    expression = value.strip()[2:-2]
                    if expression.startswith("-"):
                        expression = expression[1:]
                    if expression.endswith("-"):
                        expression = expression[:-1]
                    expression = expression.strip()
                    result = self._environment.compile_expression(expression)()
                else:
                    result = self._environment.from_string(value).render()
            else:
                result = self._environment.from_string(value).render()
        except Exception as exc:
            if self._secret_accessed:
                raise ValueError("Could not render a secret profile variable") from None
            raise ValueError(f"Could not render profile value: {exc}") from exc
        if self._secret_accessed and not direct_env_var:
            raise ValueError(
                "Secret environment variables cannot be modified in profile Jinja"
            )
        return result

    def render(self, value: Any) -> Any:
        if isinstance(value, dict):
            return {key: self.render(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self.render(item) for item in value]
        return self.render_scalar(value)


def _parse_yaml(content: str, filename: str) -> Dict[str, Any]:
    data = yaml.safe_load(content)
    if not isinstance(data, dict):
        raise ValueError(f"{filename} must contain a YAML mapping")
    return data


def load_selected_output(
    project_dir: str,
    profiles_dir: str,
    profile_override: Optional[str] = None,
    target_override: Optional[str] = None,
    cli_vars: Optional[Mapping[str, Any]] = None,
) -> Output:
    renderer = ProfileRenderer(cli_vars)
    project_path = Path(project_dir) / "dbt_project.yml"
    project = _parse_yaml(project_path.read_text(), str(project_path))
    profile_name = profile_override or renderer.render_scalar(project.get("profile"))
    if not isinstance(profile_name, str) or not profile_name:
        raise ValueError("dbt_project.yml must select a profile, or pass --profile")

    profiles_path = Path(profiles_dir) / "profiles.yml"
    profiles = _parse_yaml(profiles_path.read_text(), str(profiles_path))
    if profile_name not in profiles:
        raise ValueError(f"Profile {profile_name!r} is not present in profiles.yml")
    profile = profiles[profile_name]
    if not isinstance(profile, dict) or not isinstance(profile.get("outputs"), dict):
        raise ValueError(f"Profile {profile_name!r} must contain outputs")
    target = target_override or renderer.render_scalar(profile.get("target"))
    if target not in profile["outputs"]:
        raise ValueError(
            f"Target {target!r} is not present in profile {profile_name!r}"
        )
    try:
        return Output.model_validate(renderer.render(profile["outputs"][target]))
    except ValidationError as exc:
        fields = ", ".join(".".join(map(str, error["loc"])) for error in exc.errors())
        raise ValueError(
            f"Invalid BigQuery output {target!r}; check fields: {fields}"
        ) from None
