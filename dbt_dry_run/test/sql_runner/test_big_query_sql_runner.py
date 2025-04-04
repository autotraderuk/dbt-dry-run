from contextlib import contextmanager
from typing import Generator, cast
from unittest.mock import MagicMock

import pytest
from google.api_core.exceptions import BadRequest
from google.cloud.bigquery import DatasetReference, SchemaField, Table, TableReference
from google.cloud.exceptions import NotFound
from tenacity import RetryError, wait_none

from dbt_dry_run.adapter.service import ProjectService
from dbt_dry_run.exception import UnknownSchemaException
from dbt_dry_run.results import DryRunStatus
from dbt_dry_run.sql_runner.big_query_sql_runner import (
    MAX_ATTEMPT_NUMBER,
    QUERY_TIMED_OUT,
    BigQuerySQLRunner,
)
from dbt_dry_run.test.utils import SimpleNode


class MockProject:
    def __init__(self) -> None:
        self._connection_mock = MagicMock()
        self.mock_client = MagicMock()
        self._connection_mock.handle = self.mock_client

    def get_connection(self) -> MagicMock:
        return self._connection_mock

    def assert_query_called_with_sql(self, sql: str, num_calls: int = 1) -> None:
        assert len(self.mock_client.query.mock_calls) == num_calls
        assert all(call.args[0] == sql for call in self.mock_client.query.mock_calls)


def test_timeout_query_retries() -> None:
    mock_project = MockProject()
    bad_request: BadRequest = BadRequest(message=QUERY_TIMED_OUT)
    mock_project.mock_client.query.side_effect = bad_request
    sql_runner = BigQuerySQLRunner(cast(ProjectService, mock_project))

    # Disable wait to make test run faster
    sql_runner.query.retry.wait = wait_none()  # type: ignore

    expected_sql = "SELECT * FROM foo"
    with pytest.raises(RetryError):
        sql_runner.query(expected_sql)

    mock_project.assert_query_called_with_sql(expected_sql, MAX_ATTEMPT_NUMBER)


def test_error_query_does_not_retry() -> None:
    mock_project = MockProject()
    raised_exception = BadRequest(message="FOO")
    mock_project.mock_client.query.side_effect = raised_exception
    sql_runner = BigQuerySQLRunner(cast(ProjectService, mock_project))

    expected_sql = "SELECT * FROM foo"
    status, _, exc = sql_runner.query(expected_sql)

    assert status == DryRunStatus.FAILURE
    assert exc is raised_exception

    mock_project.assert_query_called_with_sql(expected_sql)


def test_get_node_schema_returns_none_if_not_found() -> None:
    mock_project = MockProject()
    raised_exception = NotFound("not_found")
    mock_project.mock_client.get_table.side_effect = raised_exception
    sql_runner = BigQuerySQLRunner(cast(ProjectService, mock_project))

    example_node = SimpleNode(unique_id="a", depends_on=[]).to_node()
    table = sql_runner.get_node_schema(example_node)

    assert table is None
    assert len(mock_project.mock_client.get_table.mock_calls) == 1

    actual_table_ref = mock_project.mock_client.get_table.mock_calls[0].args[0]
    expected_table_ref = TableReference(
        DatasetReference(example_node.database, example_node.db_schema),
        example_node.alias,
    )
    assert actual_table_ref == expected_table_ref


def test_get_node_schema_returns_table_schema() -> None:
    mock_project = MockProject()
    example_node = SimpleNode(unique_id="a", depends_on=[]).to_node()
    expected_table_ref = TableReference(
        DatasetReference(example_node.database, example_node.db_schema),
        example_node.alias,
    )

    a_schema_field = SchemaField(name="a", field_type="NUMERIC")
    mock_project.mock_client.get_table.return_value = Table(
        table_ref=expected_table_ref, schema=[a_schema_field]
    )
    sql_runner = BigQuerySQLRunner(cast(ProjectService, mock_project))

    table = sql_runner.get_node_schema(example_node)

    assert table is not None
    assert len(mock_project.mock_client.get_table.mock_calls) == 1

    table_column_names = set(field.name for field in table.fields)
    assert table_column_names == set("a")


def test_get_schema_from_schema_fields_raises_error_if_unknown_field_type() -> None:
    invalid_field_type = "INVALID_FIELD_TYPE"
    invalid_field_name = "a"
    expected_error_message = f"BigQuery dry run field '{invalid_field_name}' returned unknown column types: '{invalid_field_type}' is not a valid BigQueryFieldType"
    with pytest.raises(UnknownSchemaException, match=expected_error_message):
        BigQuerySQLRunner.get_schema_from_schema_fields(
            [SchemaField(name=invalid_field_name, field_type=invalid_field_type)]
        )
