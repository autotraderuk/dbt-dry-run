import argparse
import os
from typing import Dict

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

PROFILE_FILENAME = "profiles.yml"


def read_profiles(path: str) -> Dict[str, Profile]:
    all_profiles: Dict[str, Profile] = {}
    with open(path) as f:
        profile_data = yaml.safe_load(f)
    for name, profile in profile_data.items():
        if name != "config":
            all_profiles[name] = Profile(**profile)
    return all_profiles


def run() -> int:
    parsed_args = parser.parse_args()
    manifest = read_manifest(parsed_args.manifest_path)
    profiles = read_profiles(os.path.join(parsed_args.profiles_dir, PROFILE_FILENAME))
    profile = profiles[parsed_args.profile]
    active_output = parsed_args.target or profile.target
    output = profile.outputs[active_output]

    dry_run_results = dry_run_manifest(manifest, output, parsed_args.model)

    exit_code = ResultReporter(
        dry_run_results, set(), parsed_args.verbose
    ).report_and_check_results()
    if parsed_args.ignore_result:
        exit_code = 0
    return exit_code
