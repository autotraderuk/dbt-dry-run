from typing import Optional, Tuple, cast
from unittest.mock import MagicMock

import google
import google.auth.credentials
import pytest
from google.api_core.exceptions import BadRequest
from google.oauth2.service_account import Credentials
from pytest_mock.plugin import MockerFixture
from tenacity import RetryError, wait_none

from dbt_dry_run.models import DryRunStatus, Output
from dbt_dry_run.sql_runner.big_query_sql_runner import (
    MAX_ATTEMPT_NUMBER,
    QUERY_TIMED_OUT,
    BigQuerySQLRunner,
)


def _oauth_creds() -> Tuple[google.auth.credentials.Credentials, Optional[str]]:
    return cast(MagicMock, google.auth.credentials.Credentials), "project_id"


def _service_account_creds() -> google.oauth2.service_account.Credentials:
    return cast(MagicMock, google.oauth2.service_account.Credentials)


def test_from_profile_with_oauth_impersonating_service_account_credentials(
    mocker: MockerFixture,
) -> None:
    mock = mocker.patch("google.auth.default")
    mock.return_value = _oauth_creds()

    dbt_profile_config = {
        "type": "bigquery",
        "method": "oauth",
        "project": "admin-project",
        "schema": "core",
        "location": "EU",
        "threads": 8,
        "timeout_seconds": 300,
        "keyfile": "some_path_to_key_file.json",
        "impersonate_service_account": "data-product@dbt.iam.gserviceaccount.com",
    }
    output = Output(**dbt_profile_config)

    actual = BigQuerySQLRunner.from_profile(output)

    assert actual.client.project == "admin-project"
    assert (
        actual.client._credentials.service_account_email
        == "data-product@dbt.iam.gserviceaccount.com"
    )


def test_from_profile_with_service_account_impersonating_service_account_credentials(
    mocker: MockerFixture,
) -> None:
    mock = mocker.patch(
        "google.oauth2.service_account.Credentials.from_service_account_file"
    )
    mock.return_value = _service_account_creds()

    dbt_profile_config = {
        "type": "bigquery",
        "method": "service-account",
        "project": "admin-project",
        "schema": "core",
        "location": "EU",
        "threads": 8,
        "timeout_seconds": 300,
        "keyfile": "some_path_to_key_file.json",
        "impersonate_service_account": "data-product@dbt.iam.gserviceaccount.com",
    }
    output = Output(**dbt_profile_config)
    actual = BigQuerySQLRunner.from_profile(output)

    assert actual.client.project == "admin-project"
    assert (
        actual.client._credentials.service_account_email
        == "data-product@dbt.iam.gserviceaccount.com"
    )


def test_from_profile_with_oauth_credentials(mocker: MockerFixture) -> None:
    mock_creds = _oauth_creds()
    mock = mocker.patch("google.auth.default")
    mock.return_value = mock_creds
    mock_client = mocker.patch("dbt_dry_run.sql_runner.big_query_sql_runner.Client")

    dbt_profile_config = {
        "type": "bigquery",
        "method": "oauth",
        "project": "admin-project",
        "schema": "core",
        "location": "EU",
        "threads": 8,
        "timeout_seconds": 300,
        "keyfile": "some_path_to_key_file.json",
    }
    output = Output(**dbt_profile_config)

    BigQuerySQLRunner.from_profile(output)

    name, args, kwargs = mock_client.mock_calls[0]
    assert kwargs["project"] == "admin-project"
    assert kwargs["location"] == "EU"
    assert kwargs["credentials"] == mock_creds[0]


def test_from_profile_with_service_account_credentials(mocker: MockerFixture) -> None:
    mock_creds = _service_account_creds()
    mock = mocker.patch(
        "google.oauth2.service_account.Credentials.from_service_account_file"
    )
    mock.return_value = mock_creds
    mock_client = mocker.patch("dbt_dry_run.sql_runner.big_query_sql_runner.Client")

    dbt_profile_config = {
        "type": "bigquery",
        "method": "service-account",
        "project": "admin-project",
        "schema": "core",
        "location": "EU",
        "threads": 8,
        "timeout_seconds": 300,
        "keyfile": "some_path_to_key_file.json",
    }
    output = Output(**dbt_profile_config)
    BigQuerySQLRunner.from_profile(output)

    name, args, kwargs = mock_client.mock_calls[0]
    assert kwargs["project"] == "admin-project"
    assert kwargs["location"] == "EU"
    assert kwargs["credentials"] == mock_creds


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
