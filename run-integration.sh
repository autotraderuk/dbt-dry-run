#!/bin/bash
set -e
project_name=$1
command=${2:-compile}
dbt "$command" --profiles-dir ./integration/profiles --target integration-local --project-dir ./integration/projects/"$project_name"
python3 -m dbt_dry_run default --profiles-dir ./integration/profiles --target integration-local --manifest-path ./integration/projects/"$project_name"/target/manifest.json --report-path ./integration/projects/"$project_name"/target/dry_run.json
