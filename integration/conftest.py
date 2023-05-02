import os
import subprocess
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Optional, Iterable, Generator

import pytest
from _pytest.fixtures import FixtureRequest

from dbt_dry_run.models import Report, Node, Manifest
from dbt_dry_run.adapter.service import ProjectService, DbtArgs
from google.cloud.bigquery import Client


@dataclass
class DryRunResult:
    process: subprocess.CompletedProcess
    report: Optional[Report]


class ProjectContext:
    def __init__(self, project_dir: str, profiles_dir: str, target: str):
        self.project_dir = project_dir
        self.profiles_dir = profiles_dir
        self.target = target
        args = DbtArgs(
            profiles_dir=profiles_dir, project_dir=project_dir, target=target
        )
        self._project = ProjectService(args)
        self._manifest: Optional[Manifest] = None

    @contextmanager
    def create_state(
        self, node: Node, columns: Iterable[str]
    ) -> Generator[None, None, None]:
        node_name = node.to_table_ref_literal()
        schema_csv = ",\n".join(columns)
        create_ddl = f"""
            CREATE OR REPLACE TABLE {node_name}
            (
                {schema_csv}
            );
        """
        client: Client = self._project.get_connection().handle
        client.query(create_ddl)
        yield
        drop_ddl = f"""
            DROP TABLE {node_name};
        """
        client.query(drop_ddl)

    @property
    def manifest(self) -> Manifest:
        if self._manifest:
            return self._manifest
        self._manifest = self._project.get_dbt_manifest()
        return self._manifest

    def dry_run(self, skip_not_compiled: bool = False) -> DryRunResult:
        target_dir = os.path.join(self.project_dir, "target")
        report_path = os.path.join(target_dir, "dry_run_output.json")
        if os.path.exists(report_path):
            os.remove(report_path)

        dry_run_args = [
            "python3",
            "-m",
            "dbt_dry_run",
            "--project-dir",
            f"{self.project_dir}",
            "--profiles-dir",
            self.profiles_dir,
            "--target",
            self.target,
            "--report-path",
            report_path,
        ]
        if skip_not_compiled:
            dry_run_args.append("--skip-not-compiled")
        run_dry_run = subprocess.run(dry_run_args, capture_output=True)

        if os.path.exists(report_path):
            dry_run_report = Report.parse_file(report_path)
        else:
            dry_run_report = None

        return DryRunResult(run_dry_run, dry_run_report)


def running_in_github() -> bool:
    return os.environ.get("GITHUB_ACTIONS", "not-set") == "true"


def _dry_run_result(
    project: ProjectContext, skip_not_compiled: bool = False
) -> DryRunResult:
    return project.dry_run(skip_not_compiled)


@pytest.fixture(scope="module")
def dry_run_result_skip_not_compiled(compiled_project: ProjectContext) -> DryRunResult:
    yield _dry_run_result(compiled_project, True)


@pytest.fixture(scope="module")
def dry_run_result(compiled_project: ProjectContext) -> DryRunResult:
    yield _dry_run_result(compiled_project)


@pytest.fixture(scope="module")
def compiled_project(request: FixtureRequest) -> ProjectContext:
    folder = request.fspath.dirname
    profiles_dir = os.path.join(request.config.rootdir, "integration/profiles")
    target_path = os.path.join(folder, "target")
    if running_in_github():
        target = "integration-github"
    else:
        target = "integration-local"
    run_dbt = subprocess.run(
        [
            "dbt",
            "compile",
            "--project-dir",
            f"{folder}",
            "--profiles-dir",
            profiles_dir,
            "--target",
            target,
            "--target-path",
            target_path,
        ],
        capture_output=True,
    )
    test_display_name = f"{request.keywords.node.name}/{request.node.name}"

    dbt_stdout = run_dbt.stdout.decode("utf-8")
    if run_dbt.returncode != 0:
        raise RuntimeError(
            f"dbt has failed to compile for test '{test_display_name}' due to:\n"
            f" {dbt_stdout}\n"
            f"Fix dbt compilation error to run test suite!",
            run_dbt.returncode,
        )
    return ProjectContext(project_dir=folder, profiles_dir=profiles_dir, target=target)
