import os
from typing import Optional

import typer
from dbt.flags import DEFAULT_PROFILES_DIR
from typer import Option

from dbt_dry_run.adapter.service import DbtArgs, ProjectService
from dbt_dry_run.exception import ManifestValidationError
from dbt_dry_run.execution import dry_run_manifest
from dbt_dry_run.flags import Flags, set_flags
from dbt_dry_run.result_reporter import ResultReporter

app = typer.Typer()


def dry_run(
    project_dir: str,
    profiles_dir: str,
    target: Optional[str],
    verbose: bool = False,
    report_path: Optional[str] = None,
    cli_vars: str = "{}",
    skip_not_compiled: bool = False,
) -> int:
    set_flags(Flags(skip_not_compiled=skip_not_compiled))
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


_SKIP_NOT_COMPILED_HELP = """
    Whether or not the dry run should ignore models that are not compiled. This has several caveats that make this 
    not a recommended option. The dbt manifest should generally be compiled with `--select *` to ensure good 
    coverage
"""


@app.command()
def run(
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
    skip_not_compiled: bool = Option(
        False, "--skip-not-compiled", help=_SKIP_NOT_COMPILED_HELP
    ),
) -> None:
    exit_code = dry_run(
        project_dir, profiles_dir, target, verbose, report_path, vars, skip_not_compiled
    )
    if exit_code > 0:
        raise typer.Exit(exit_code)


if __name__ == "__main__":
    app()
