import json
import os
from typing import Optional

import typer
from typer import Option

from dbt_dry_run.adapter.service import DbtArgs, ProjectService
from dbt_dry_run.adapter.utils import default_profiles_dir
from dbt_dry_run.exception import ManifestValidationError
from dbt_dry_run.execution import dry_run_manifest
from dbt_dry_run.flags import Flags, set_flags
from dbt_dry_run.result_reporter import ResultReporter
from dbt_dry_run.version import VERSION

app = typer.Typer()


def dry_run(
    project_dir: str,
    profiles_dir: str,
    target: Optional[str],
    target_path: Optional[str],
    verbose: bool = False,
    report_path: Optional[str] = None,
    cli_vars: str = "{}",
    skip_not_compiled: bool = False,
    full_refresh: bool = False,
    extra_check_columns_metadata_key: Optional[str] = None,
    threads: Optional[int] = None,
) -> int:
    cli_vars_parsed = json.loads(cli_vars)
    set_flags(
        Flags(
            skip_not_compiled=skip_not_compiled,
            full_refresh=full_refresh,
            extra_check_columns_metadata_key=extra_check_columns_metadata_key,
        )
    )
    args = DbtArgs(
        project_dir=project_dir,
        profiles_dir=os.path.abspath(profiles_dir),
        target=target,
        target_path=target_path,
        vars=cli_vars_parsed,
        threads=threads,
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

_EXTRA_CHECK_COLUMNS_METADATA_KEY_HELP = """
    An extra metadata key that can be used in place of `dry_run.check_columns` for verifying column metadata has been
    specified correctly. `dry_run.check_columns` will always take precedence. The metadata key should be of boolean type
    or it will be cast to a boolean to be 'True/Falsey`
"""

_THREADS_HELP = """
"[dbt] Number of threads to execute DAG with. You can normally set this higher than the concurrency of your actual dbt
runs because the dry run queries execute much faster and don't use any resources"
"""


def version_callback(value: bool) -> None:
    if value:
        print(f"dbt-dry-run v{VERSION}")
        raise typer.Exit()


@app.command()
def run(
    profiles_dir: str = Option(
        default_profiles_dir(), help="[dbt] Where to search for `profiles.yml`"
    ),
    project_dir: str = Option(
        os.getcwd(), help="[dbt] Where to search for `dbt_project.yml`"
    ),
    vars: str = Option("{}", help="[dbt] CLI Variables to pass to dbt"),
    target: Optional[str] = Option(None, help="[dbt] Target profile"),
    target_path: Optional[str] = Option(None, help="[dbt] Target path"),
    threads: Optional[int] = Option(None, help=_THREADS_HELP),
    verbose: bool = Option(False, help="Output verbose error messages"),
    report_path: Optional[str] = Option(None, help="Json path to dump report to"),
    skip_not_compiled: bool = Option(
        False, "--skip-not-compiled", help=_SKIP_NOT_COMPILED_HELP
    ),
    full_refresh: bool = Option(False, "--full-refresh", help="[dbt] Full refresh"),
    extra_check_columns_metadata_key: Optional[str] = Option(
        None,
        "--extra-check-columns-metadata-key",
        help=_EXTRA_CHECK_COLUMNS_METADATA_KEY_HELP,
    ),
    _: Optional[bool] = Option(None, "--version", callback=version_callback),
) -> None:
    exit_code = dry_run(
        project_dir,
        profiles_dir,
        target,
        target_path,
        verbose,
        report_path,
        vars,
        skip_not_compiled,
        full_refresh,
        extra_check_columns_metadata_key,
        threads,
    )
    if exit_code > 0:
        raise typer.Exit(exit_code)


if __name__ == "__main__":
    app()
