import base64
import os
import subprocess
from dataclasses import dataclass
from typing import Optional

import pytest
from _pytest.fixtures import FixtureRequest

from dbt_dry_run.models import Report

SVC_ACCOUNT_ENV = "DRY_RUN_SVC_JSON"
SERVICE_ACCOUNT_JSON_FILEPATH = os.environ.get("HOME", "/home") + "/bq_svc.json"


@dataclass
class IntegrationTestResult:
    process: subprocess.CompletedProcess
    report: Optional[Report]


def running_in_github() -> bool:
    return os.environ.get("GITHUB_ACTIONS", 'not-set') == 'true'


def pass_through_credentials():
    try:
        service_account_json_b64 = os.environ[SVC_ACCOUNT_ENV]
        if len(service_account_json_b64) == 0:
            raise ValueError("SVC Account JSON empty")
    except KeyError:
        raise KeyError(f"Could not find environment variable '{SVC_ACCOUNT_ENV}' to pass through credentials")
    try:
        service_account_json = base64.b64decode(service_account_json_b64).decode('utf-8')
    except Exception:
        raise ValueError("Failed to decode service account JSON")
    with open(SERVICE_ACCOUNT_JSON_FILEPATH, 'w') as f:
        f.write(service_account_json)


@pytest.fixture(scope="module")
def dry_run_result(request: FixtureRequest) -> IntegrationTestResult:
    folder = request.fspath.dirname
    profiles_dir = os.path.join(request.config.rootdir, "integration/profiles")
    report_path = os.path.join(folder, "target/dry_run_output.json")
    if running_in_github():
        target = "integration-github"
        # pass_through_credentials()
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
    run_dry_run = subprocess.run(
        ["python3", "-m", "dbt_dry_run",
         "--manifest-path", f"{manifest_path}",
         "--profiles-dir", profiles_dir,
         "--target", target,
         '--report-path', report_path,
         "default"], capture_output=True)

    if os.path.exists(report_path):
        dry_run_report = Report.parse_file(report_path)
    else:
        dry_run_report = None

    yield IntegrationTestResult(run_dry_run, dry_run_report)
