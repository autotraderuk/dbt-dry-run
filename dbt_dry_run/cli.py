import os
from typing import Optional

import typer
from dbt.flags import DEFAULT_PROFILES_DIR
from typer import Argument, Option

from dbt_dry_run.adapter.service import DbtArgs, ProjectService
from dbt_dry_run.exception import ManifestValidationError
from dbt_dry_run.execution import dry_run_manifest
from dbt_dry_run.result_reporter import ResultReporter

app = typer.Typer()


def dry_run(
    project_dir: str,
    profiles_dir: str,
    target: Optional[str],
    verbose: bool = False,
    report_path: Optional[str] = None,
    cli_vars: str = "{}",
) -> int:
    args = DbtArgs(
        project_dir=project_dir,
        profiles_dir=os.path.abspath(profiles_dir),
        target=target,
        vars=cli_vars,
    )
    project = ProjectService(args)
    exit_code: int
    try:
        dry_run_results = dry_run_manifest(project)
        reporter = ResultReporter(dry_run_results, set(), verbose)
        exit_code = reporter.report_and_check_results()

        report = reporter.get_report()

        if report_path:
            with open(report_path, "w") as f:
                f.write(report.json(by_alias=True))

    except ManifestValidationError as e:
        print("Dry run failed to validate manifest")
        print(str(e))
        exit_code = 1
    return exit_code


@app.command()
def run(
    profile: Optional[str] = Argument(
        None,
        hidden=True,
        help="Legacy parameter. You should not use this anymore see CHANGES.md in the github repo for how to migrate",
    ),
    profiles_dir: str = Option(
        DEFAULT_PROFILES_DIR, help="[dbt] Where to search for `profiles.yml`"
    ),
    project_dir: str = Option(
        os.getcwd(), help="[dbt] Where to search for `dbt_project.yml`"
    ),
    vars: str = Option("{}", help="[dbt] CLI Variables to pass to dbt"),
    target: Optional[str] = Option(None, help="[dbt] Target profile"),
    verbose: bool = Option(False, help="Output verbose error messages"),
    report_path: Optional[str] = Option(None, help="Json path to dump report to"),
) -> None:
    if profile is not None:
        print(
            "CLI format has changed see CHANGES.md v0.4.0 for instructions on how to migrate"
        )
        raise typer.Exit(1)
    exit_code = dry_run(project_dir, profiles_dir, target, verbose, report_path, vars)
    if exit_code > 0:
        raise typer.Exit(exit_code)


if __name__ == "__main__":
    app()
