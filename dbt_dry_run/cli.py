import argparse
import os
from typing import Dict, Optional

import jinja2
import yaml

from dbt_dry_run.execution import dry_run_manifest
from dbt_dry_run.manifest import read_manifest
from dbt_dry_run.models import Profile
from dbt_dry_run.result_reporter import ResultReporter

parser = argparse.ArgumentParser(description="Dry run DBT")
parser.add_argument(
    "profile", metavar="PROFILE", type=str, help="The profile to dry run against"
)
parser.add_argument(
    "--manifest-path",
    default="manifest.json",
    help="The location of the compiled manifest.json",
)
parser.add_argument("--target", type=str, help="The target to dry run against")
parser.add_argument(
    "--profiles-dir",
    type=str,
    default="~/.dbt/",
    help="Override default profiles directory from ~/.dbt",
)
parser.add_argument(
    "--ignore-result",
    action="store_true",
    help="Always exit 0 even if there are failures",
)
parser.add_argument(
    "--model", help="Only dry run this model and its upstream dependencies"
)
parser.add_argument(
    "--verbose", action="store_true", help="Output verbose error messages"
)
parser.add_argument("--report-path", type=str, help="Json path to dump report to")

PROFILE_FILENAME = "profiles.yml"


def profiles_get_env_var(key: str, default: str = None) -> Optional[str]:
    return os.environ.get(key, default)


def read_profiles(path: str) -> Dict[str, Profile]:
    all_profiles: Dict[str, Profile] = {}

    profile_filepath = os.path.join(path, PROFILE_FILENAME)
    if not os.path.exists(profile_filepath):
        raise FileNotFoundError(
            f"Could not find '{PROFILE_FILENAME}' at '{profile_filepath}'"
        )

    template_loader = jinja2.FileSystemLoader(searchpath=path)
    template_env = jinja2.Environment(loader=template_loader)
    template_env.globals.update(env_var=profiles_get_env_var)
    template = template_env.get_template(PROFILE_FILENAME)
    output_text = template.render()
    profile_data = yaml.safe_load(output_text)
    for name, profile in profile_data.items():
        if name != "config":
            all_profiles[name] = Profile(**profile)
    return all_profiles


def run() -> int:
    parsed_args = parser.parse_args()
    manifest = read_manifest(parsed_args.manifest_path)
    profiles = read_profiles(parsed_args.profiles_dir)
    try:
        profile = profiles[parsed_args.profile]
    except KeyError:
        raise KeyError(
            f"Could not find profile '{parsed_args.profile}' in profiles: {list(profiles.keys())}"
        )

    active_output = parsed_args.target or profile.target
    try:
        output = profile.outputs[active_output]
    except KeyError:
        raise KeyError(
            f"Could not find target `{active_output}` in outputs: {list(profile.outputs.keys())}"
        )

    dry_run_results = dry_run_manifest(manifest, output, parsed_args.model)

    reporter = ResultReporter(dry_run_results, set(), parsed_args.verbose)
    exit_code = reporter.report_and_check_results()
    if parsed_args.report_path:
        reporter.write_results_artefact(parsed_args.report_path)

    if parsed_args.ignore_result:
        exit_code = 0
    return exit_code
