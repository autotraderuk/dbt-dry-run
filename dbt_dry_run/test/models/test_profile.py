from pathlib import Path

import pytest

from dbt_dry_run.adapter.profile import load_selected_output
from dbt_dry_run.models.profile import Output


def selected_output(profile_string: str, tmp_path: Path) -> Output:
    (tmp_path / "dbt_project.yml").write_text("profile: default\n")
    (tmp_path / "profiles.yml").write_text(profile_string)
    return load_selected_output(str(tmp_path), str(tmp_path))


def test_as_number_filter(tmp_path: Path) -> None:
    profile_string = """
    default:
        target: test-output

        outputs:
            test-output:
              type: bigquery
              method: oauth
              project:  my_project
              schema: dry_run
              location: EU
              threads: "{{ '8' | as_number }}"
              timeout_seconds: 300
    """

    assert selected_output(profile_string, tmp_path).threads == 8


def test_env_var_filter(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    expected_location = "LOCATION_TEST"
    monkeypatch.setenv("DBT_DRY_RUN_TEST_LOCATION", expected_location)
    profile_string = """
    default:
        target: test-output

        outputs:
            test-output:
              type: bigquery
              method: oauth
              project:  my_project
              schema: dry_run
              location: "{{ env_var('DBT_DRY_RUN_TEST_LOCATION') }}"
              threads: 4
              timeout_seconds: 300
    """

    assert selected_output(profile_string, tmp_path).location == expected_location


def test_dataset_schema_alias(tmp_path: Path) -> None:
    profile_string = """
    default:
        target: test-output

        outputs:
            test-output:
              type: bigquery
              method: oauth
              project:  my_project
              schema: dry_run_schema
              location: EU
              threads: 4
              timeout_seconds: 300
    """

    assert selected_output(profile_string, tmp_path).dataset == "dry_run_schema"

    profile_string = """
    default:
        target: test-output

        outputs:
            test-output:
              type: bigquery
              method: oauth
              project:  my_project
              dataset: dry_run_dataset
              location: EU
              threads: 4
              timeout_seconds: 300
    """

    assert selected_output(profile_string, tmp_path).dataset == "dry_run_dataset"

    profile_string = """
    default:
        target: test-output

        outputs:
            test-output:
              type: bigquery
              method: oauth
              project:  my_project
              dataset: dry_run_dataset
              schema: dry_run_schema
              location: EU
              threads: 4
              timeout_seconds: 300
    """

    with pytest.raises(ValueError):
        selected_output(profile_string, tmp_path)
