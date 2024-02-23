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
    def __init__(
        self, project_dir: str, profiles_dir: str, target: str, target_path: str
    ):
        self.project_dir = project_dir
        self.profiles_dir = profiles_dir
        self.target = target
        self.target_path = target_path
        args = DbtArgs(
            profiles_dir=profiles_dir,
            target_path=target_path,
            project_dir=project_dir,
            target=target,
        )
        self._project = ProjectService(args)
        self._manifest: Optional[Manifest] = None

    @contextmanager
    def create_state(
        self,
        node: Node,
        columns: Iterable[str],
        partition_by: Optional[str] = None,
        require_partition_by: bool = False,
    ) -> Generator[None, None, None]:
        node_name = node.to_table_ref_literal()
        schema_csv = ",\n".join(columns)
        partition_by_clause = f"""
        PARTITION BY {partition_by}
        OPTIONS (require_partition_filter = {require_partition_by})
        """
        if not partition_by:
            partition_by_clause = ""
        create_ddl = f"""
        CREATE OR REPLACE TABLE {node_name}
            (
                {schema_csv}
            )
            {partition_by_clause};
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

    def dry_run(
        self, skip_not_compiled: bool = False, full_refresh: bool = False
    ) -> DryRunResult:
        report_path = os.path.join(self.target_path, "dry_run_output.json")
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
            "--target-path",
            self.target_path,
            "--report-path",
            report_path,
        ]
        if skip_not_compiled:
            dry_run_args.append("--skip-not-compiled")
        if full_refresh:
            dry_run_args.append("--full-refresh")
        run_dry_run = subprocess.run(dry_run_args, capture_output=True)

        if os.path.exists(report_path):
            dry_run_report = Report.parse_file(report_path)
        else:
            dry_run_report = None

        return DryRunResult(run_dry_run, dry_run_report)


def running_in_github() -> bool:
    return os.environ.get("GITHUB_ACTIONS", "not-set") == "true"


def _dry_run_result(
    project: ProjectContext, skip_not_compiled: bool = False, full_refresh: bool = False
) -> DryRunResult:
    return project.dry_run(skip_not_compiled, full_refresh)


@pytest.fixture(scope="module")
def dry_run_result_skip_not_compiled(compiled_project: ProjectContext) -> DryRunResult:
    yield _dry_run_result(compiled_project, skip_not_compiled=True)


@pytest.fixture(scope="module")
def dry_run_result_full_refresh(compiled_project: ProjectContext) -> DryRunResult:
    yield _dry_run_result(compiled_project, full_refresh=True)


@pytest.fixture(scope="module")
def dry_run_result(compiled_project: ProjectContext) -> DryRunResult:
    yield _dry_run_result(compiled_project)


@pytest.fixture(scope="module")
def compiled_project(request: FixtureRequest) -> ProjectContext:
    return _compiled_project(request, full_refresh=False)


@pytest.fixture(scope="module")
def compiled_project_full_refresh(request: FixtureRequest) -> ProjectContext:
    return _compiled_project(request, full_refresh=True)


def _compiled_project(
    request: FixtureRequest, full_refresh: bool = False
) -> ProjectContext:
    folder = request.fspath.dirname
    profiles_dir = os.path.join(request.config.rootdir, "integration/profiles")
    target_path = os.path.join(folder, "target")
    if full_refresh:
        target_path = os.path.join(folder, "target-full-refresh")
    if running_in_github():
        target = "integration-github"
    else:
        target = "integration-local"
    dbt_args = [
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
    ]
    if full_refresh:
        dbt_args.append("--full-refresh")
    run_dbt = subprocess.run(
        dbt_args,
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
    return ProjectContext(
        project_dir=folder,
        profiles_dir=profiles_dir,
        target=target,
        target_path=target_path,
    )
