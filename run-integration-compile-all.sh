#!/bin/bash
set -u -o pipefail

projects_root="./integration/projects"
profiles_dir="./integration/profiles"
target_name="integration-local"

if [[ ! -d "$projects_root" ]]; then
  echo "Projects directory not found: $projects_root"
  exit 2
fi

overall_exit=0
success_count=0
failure_count=0

shopt -s nullglob
project_dirs=("$projects_root"/*/)
shopt -u nullglob

if [[ ${#project_dirs[@]} -eq 0 ]]; then
  echo "No project directories found under $projects_root"
  exit 0
fi

for project_dir in "${project_dirs[@]}"; do
  project_name="$(basename "$project_dir")"

  echo "============================================================"
  echo "Compiling project: $project_name"

  dbt compile \
    --profiles-dir "$profiles_dir" \
    --target "$target_name" \
    --project-dir "$project_dir" \
    --target-path target\
    --static-analysis off

  exit_code=$?

  if [[ $exit_code -eq 0 ]]; then
    echo "RESULT: $project_name -> OK (exit 0)"
    success_count=$((success_count + 1))
  else
    echo "RESULT: $project_name -> FAIL (exit $exit_code)"
    failure_count=$((failure_count + 1))
    overall_exit=1
  fi
done

echo "============================================================"
echo "Compile summary:"
echo "  Success: $success_count"
echo "  Failure: $failure_count"

exit $overall_exit
