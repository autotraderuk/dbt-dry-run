#!/bin/bash
set -e
project_name=$1
command=${2:-compile}
dbt "$command" --profiles-dir ./integration/profiles --target integration-local --project-dir ./integration/projects/"$project_name" --target-path target --static-analysis off
python3 -m dbt_dry_run --profiles-dir ./integration/profiles --target integration-local --project-dir ./integration/projects/"$project_name" --report-path ./integration/projects/"$project_name"/target/dry_run_adhoc.json
