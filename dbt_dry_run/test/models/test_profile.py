import os

import pytest

from dbt_dry_run.models.profile import read_profiles


def test_as_number_filter() -> None:
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

    profiles = read_profiles(profile_string)
    assert profiles["default"].outputs["test-output"].threads == 8


def test_env_var_filter() -> None:
    expected_location = "LOCATION_TEST"
    os.environ["DBT_DRY_RUN_TEST_LOCATION"] = expected_location
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

    profiles = read_profiles(profile_string)
    assert profiles["default"].outputs["test-output"].location == expected_location


def test_dataset_schema_alias() -> None:
    profile_string = """
    default:
        target: test-output

        outputs:
            test-output:
              type: bigquery
              method: oauth
              project:  my_project
              schema: dry_run_schema
              location: "{{ env_var('DBT_DRY_RUN_TEST_LOCATION') }}"
              threads: 4
              timeout_seconds: 300
    """

    profiles = read_profiles(profile_string)
    assert profiles["default"].outputs["test-output"].dataset == "dry_run_schema"

    profile_string = """
    default:
        target: test-output

        outputs:
            test-output:
              type: bigquery
              method: oauth
              project:  my_project
              dataset: dry_run_dataset
              location: "{{ env_var('DBT_DRY_RUN_TEST_LOCATION') }}"
              threads: 4
              timeout_seconds: 300
    """

    profiles = read_profiles(profile_string)
    assert profiles["default"].outputs["test-output"].dataset == "dry_run_dataset"

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
              location: "{{ env_var('DBT_DRY_RUN_TEST_LOCATION') }}"
              threads: 4
              timeout_seconds: 300
    """

    with pytest.raises(ValueError):
        read_profiles(profile_string)
