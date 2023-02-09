import glob
import os
import subprocess
from dataclasses import dataclass
from typing import Optional

import pytest
from _pytest.fixtures import FixtureRequest

from dbt_dry_run.models import Report


@dataclass
class IntegrationTestResult:
    process: subprocess.CompletedProcess
    report: Optional[Report]


def running_in_github() -> bool:
    return os.environ.get("GITHUB_ACTIONS", 'not-set') == 'true'


def _dry_run_result(request: FixtureRequest, skip_not_compiled: bool = False) -> IntegrationTestResult:
    folder = request.fspath.dirname
    profiles_dir = os.path.join(request.config.rootdir, "integration/profiles")
    target_dir = os.path.join(folder, "target")
    report_path = os.path.join(target_dir, "dry_run_output.json")
    if os.path.exists(report_path):
        os.remove(report_path)
    if running_in_github():
        target = "integration-github"
    else:
        target = "integration-local"
    run_dbt = subprocess.run(
        ["dbt", "compile",
         "--project-dir", f"{folder}",
         "--profiles-dir", profiles_dir,
         "--target", target],
        capture_output=True)
    test_display_name = f"{request.keywords.node.name}/{request.node.name}"

    dbt_stdout = run_dbt.stdout.decode("utf-8")
    if run_dbt.returncode != 0:
        raise RuntimeError(f"dbt has failed to compile for test '{test_display_name}' due to:\n"
                           f" {dbt_stdout}\n"
                           f"Fix dbt compilation error to run test suite!", run_dbt.returncode)
    manifest_path = os.path.join(folder, "target/manifest.json")
    dry_run_args = ["python3", "-m", "dbt_dry_run",
                    "--project-dir", f"{folder}",
                    "--profiles-dir", profiles_dir,
                    "--target", target,
                    '--report-path', report_path]
    if skip_not_compiled:
        dry_run_args.append("--skip-not-compiled")
    run_dry_run = subprocess.run(
        dry_run_args, capture_output=True)

    if os.path.exists(report_path):
        dry_run_report = Report.parse_file(report_path)
    else:
        dry_run_report = None

    return IntegrationTestResult(run_dry_run, dry_run_report)


@pytest.fixture(scope="module")
def dry_run_result_skip_not_compiled(request: FixtureRequest) -> IntegrationTestResult:
    yield _dry_run_result(request, True)


@pytest.fixture(scope="module")
def dry_run_result(request: FixtureRequest) -> IntegrationTestResult:
    yield _dry_run_result(request)
