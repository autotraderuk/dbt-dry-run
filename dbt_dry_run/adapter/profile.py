import os
from ast import literal_eval
from typing import Any, Dict, Optional

from dbt_dry_run.models.profile import Profile
import jinja2
import yaml


def profiles_get_env_var(key: str, default: Optional[str] = None) -> Optional[str]:
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
