#!/bin/bash
set -e
project_name=$1
database=${2:-bigquery}
command=${3:-compile}
dbt "$command" --profiles-dir ./integration/profiles --target integration-local-$database --project-dir ./integration/projects/"$project_name" --target-path target
python3 -m dbt_dry_run --profiles-dir ./integration/profiles --target integration-local-$database --project-dir ./integration/projects/"$project_name" --report-path ./integration/projects/"$project_name"/target/dry_run_adhoc.json
