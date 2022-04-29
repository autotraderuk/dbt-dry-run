from unittest.mock import MagicMock

import pytest
from google.api_core.exceptions import BadRequest
from tenacity import RetryError, wait_none

from dbt_dry_run.models import DryRunStatus, Output
from dbt_dry_run.sql_runner.big_query_sql_runner import (
    MAX_ATTEMPT_NUMBER,
    QUERY_TIMED_OUT,
    BigQuerySQLRunner,
)


def test_from_profile_with_oauth_impersonating_service_account_credentials() -> None:
    dbt_profile_config = {
        'type': 'bigquery',
        'method': 'oauth',
        'project': 'admin-project',
        'schema': 'core',
        'location': 'EU',
        'threads': 8,
        'timeout_seconds': 300,
        'keyfile': 'some_path_to_key_file.json',
        'impersonate_service_account': 'data-product@dbt.iam.gserviceaccount.com'
    }
    output = Output(**dbt_profile_config)
    actual = BigQuerySQLRunner.from_profile(output)


def test_from_profile_with_service_account_impersonating_service_account_credentials() -> None:
    dbt_profile_config = {
        'type': 'bigquery',
        'method': 'service-account',
        'project': 'admin-project',
        'schema': 'core',
        'location': 'EU',
        'threads': 8,
        'timeout_seconds': 300,
        'keyfile': 'some_path_to_key_file.json',
        'impersonate_service_account': 'data-product@dbt.iam.gserviceaccount.com'
    }
    output = Output(**dbt_profile_config)
    actual = BigQuerySQLRunner.from_profile(output)


def test_from_profile_with_oauth_credentials() -> None:
    dbt_profile_config = {
        'type': 'bigquery',
        'method': 'oauth',
        'project': 'admin-project',
        'schema': 'core',
        'location': 'EU',
        'threads': 8,
        'timeout_seconds': 300,
        'keyfile': 'some_path_to_key_file.json'
    }
    output = Output(**dbt_profile_config)
    actual = BigQuerySQLRunner.from_profile(output)


def test_from_profile_with_service_account_credentials() -> None:
    dbt_profile_config = {
        'type': 'bigquery',
        'method': 'service-account',
        'project': 'admin-project',
        'schema': 'core',
        'location': 'EU',
        'threads': 8,
        'timeout_seconds': 300,
        'keyfile': 'some_path_to_key_file.json'
    }
    output = Output(**dbt_profile_config)
    actual = BigQuerySQLRunner.from_profile(output)


def test_timeout_query_retries() -> None:
    mock_client = MagicMock()
    bad_request: BadRequest = BadRequest(message=QUERY_TIMED_OUT)
    mock_client.query.side_effect = bad_request
    sql_runner = BigQuerySQLRunner(mock_client)
    # Disable wait to make test run faster
    sql_runner.query.retry.wait = wait_none()  # type: ignore

    with pytest.raises(RetryError):
        sql_runner.query("SELECT * FROM foo")

    assert len(mock_client.query.mock_calls) == MAX_ATTEMPT_NUMBER


def test_error_query_does_not_retry() -> None:
    mock_client = MagicMock()
    raised_exception = BadRequest(message="FOO")
    mock_client.query.side_effect = raised_exception
    sql_runner = BigQuerySQLRunner(mock_client)

    status, _, exc = sql_runner.query("SELECT * FROM foo")

    assert status == DryRunStatus.FAILURE
    assert exc is raised_exception

    assert len(mock_client.query.mock_calls) == 1
