from pathlib import Path
from typing import cast
from unittest.mock import MagicMock, patch

import pytest
from google.auth.credentials import CredentialsWithQuotaProject

from dbt_dry_run.adapter.bigquery import (
    EntraTokenSupplier,
    create_bigquery_client,
    create_credentials,
)
from dbt_dry_run.adapter.profile import ProfileRenderer, load_selected_output
from dbt_dry_run.adapter.service import DbtArgs, ProjectService
from dbt_dry_run.adapter.utils import default_profiles_dir
from dbt_dry_run.models.profile import Output, TokenEndpoint
from dbt_dry_run.sql_runner.big_query_sql_runner import BigQuerySQLRunner


def output(**overrides: object) -> Output:
    values: dict[str, object] = {
        "type": "bigquery",
        "method": "oauth",
        "project": "data-project",
        "schema": "analytics",
        "location": "EU",
    }
    values.update(overrides)
    return Output.model_validate(values)


def test_profile_renderer_uses_cli_vars_env_and_native_types(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DBT_THREADS", "8")
    renderer = ProfileRenderer({"region": "EU", "enabled": True})
    rendered = renderer.render(
        {
            "threads": "{{ env_var('DBT_THREADS') | as_number }}",
            "enabled": "{{ var('enabled') | as_bool }}",
            "list": "{{ '[1, 2]' | as_native }}",
            "location": "{% if var('region') == 'EU' %}EU{% else %}US{% endif %}",
            "fallback": "{{ env_var('MISSING_DBT_VALUE', 'default') }}",
            "unconverted": "{{ env_var('DBT_THREADS') }}",
        }
    )
    assert rendered == {
        "threads": 8,
        "enabled": True,
        "list": [1, 2],
        "location": "EU",
        "fallback": "default",
        "unconverted": "8",
    }
    assert renderer.render_scalar("{{- -1 -}}") == -1


def test_profile_renderer_does_not_expose_or_modify_secrets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DBT_ENV_SECRET_PASSWORD", "never-print-this")
    renderer = ProfileRenderer()
    assert (
        renderer.render_scalar("{{ env_var('DBT_ENV_SECRET_PASSWORD') }}")
        == "never-print-this"
    )
    with pytest.raises(
        ValueError, match="Secret environment variables cannot be modified"
    ) as exc:
        renderer.render_scalar("{{ env_var('DBT_ENV_SECRET_PASSWORD') | upper }}")
    assert "never-print-this" not in str(exc.value)
    with pytest.raises(
        ValueError, match="Required environment variable 'ABSENT' is not set"
    ):
        renderer.render_scalar("{{ env_var('ABSENT') }}")


def test_selected_output_uses_project_profile_and_target_overrides(
    tmp_path: Path,
) -> None:
    (tmp_path / "dbt_project.yml").write_text("profile: \"{{ var('profile') }}\"\n")
    (tmp_path / "profiles.yml").write_text(
        """default:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: dev-project
      schema: dev
    prod:
      type: bigquery
      method: service-account
      keyfile: /tmp/prod.json
      database: prod-project
      dataset: prod
other:
  target: broken
  outputs:
    broken:
      type: unsupported
"""
    )
    selected = load_selected_output(
        str(tmp_path),
        str(tmp_path),
        target_override="prod",
        cli_vars={"profile": "default"},
    )
    assert selected.project == "prod-project"
    assert selected.dataset == "prod"
    assert selected.method.value == "service-account"
    overridden = load_selected_output(
        str(tmp_path),
        str(tmp_path),
        profile_override="default",
        cli_vars={"profile": "other"},
    )
    assert overridden.project == "dev-project"


@pytest.mark.parametrize(
    "fields",
    [
        {"database": "other-project"},
        {"dataset": "other-dataset"},
        {"method": "service-account"},
        {"method": "service-account-json"},
        {"method": "oauth-secrets"},
    ],
)
def test_invalid_output_fields_fail(fields: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        output(**fields)


def test_oauth_uses_profile_scopes() -> None:
    source = MagicMock()
    with patch(
        "dbt_dry_run.adapter.bigquery.google.auth.default",
        return_value=(source, "adc-project"),
    ) as adc:
        credentials, project = create_credentials(output(scopes=["scope-one"]))
    adc.assert_called_once_with(scopes=["scope-one"])
    assert credentials is source
    assert project == "adc-project"


def test_oauth_does_not_add_scopes_to_profile() -> None:
    with patch(
        "dbt_dry_run.adapter.bigquery.google.auth.default",
        return_value=(MagicMock(), "adc-project"),
    ) as adc:
        create_credentials(output())
    adc.assert_called_once_with(scopes=None)


def test_quota_project_is_applied_to_google_credentials() -> None:
    source = MagicMock(spec=CredentialsWithQuotaProject)
    with patch(
        "dbt_dry_run.adapter.bigquery.google.auth.default",
        return_value=(source, "adc-project"),
    ):
        credentials, _ = create_credentials(output(quota_project="quota-project"))
    source.with_quota_project.assert_called_once_with("quota-project")
    assert credentials is source.with_quota_project.return_value


def test_service_account_file_credentials() -> None:
    credentials = MagicMock()
    with patch(
        "dbt_dry_run.adapter.bigquery.ServiceAccountCredentials.from_service_account_file",
        return_value=credentials,
    ) as load:
        actual, project = create_credentials(
            output(method="service-account", keyfile="/tmp/key.json")
        )
    load.assert_called_once_with("/tmp/key.json", scopes=None)
    assert actual is credentials
    assert project is None


def test_service_account_project_can_come_from_key() -> None:
    credentials = MagicMock()
    credentials.project_id = "key-project"
    with patch(
        "dbt_dry_run.adapter.bigquery.ServiceAccountCredentials.from_service_account_file",
        return_value=credentials,
    ):
        actual, project = create_credentials(
            output(method="service-account", project=None, keyfile="/tmp/key.json")
        )
    assert actual is credentials
    assert project == "key-project"


def test_wif_builds_identity_pool_credentials_without_network() -> None:
    with patch("dbt_dry_run.adapter.bigquery.IdentityPoolCredentials") as identity_pool:
        create_credentials(
            output(
                method="external-oauth-wif",
                workload_pool_provider_path="//iam.googleapis.com/projects/123/providers/test",
                token_endpoint={
                    "type": "entra",
                    "request_url": "https://login.example.com/token",
                    "request_data": "grant_type=client_credentials",
                },
                service_account_impersonation_url="https://iam.example.com/impersonate",
            )
        )
    assert identity_pool.call_args.kwargs["audience"].startswith(
        "//iam.googleapis.com/"
    )
    assert identity_pool.call_args.kwargs["service_account_impersonation_url"] == (
        "https://iam.example.com/impersonate"
    )
    identity_pool.return_value.with_scopes.assert_called_once()


def test_entra_token_supplier_fetches_and_caches_token() -> None:
    supplier = EntraTokenSupplier(
        TokenEndpoint(
            type="entra",
            request_url="https://login.example.com/token",
            request_data="grant_type=client_credentials",
        )
    )
    response = MagicMock(
        status=200, data=b'{"access_token":"signed-token","expires_in":3600}'
    )
    request = MagicMock(return_value=response)
    assert supplier.get_subject_token(None, request) == "signed-token"
    assert supplier.get_subject_token(None, request) == "signed-token"
    request.assert_called_once_with(
        url="https://login.example.com/token",
        method="POST",
        headers={
            "accept": "application/json",
            "content-type": "application/x-www-form-urlencoded",
        },
        body=b"grant_type=client_credentials",
    )


def test_profiles_dir_environment_override(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DBT_PROFILES_DIR", str(tmp_path))
    assert default_profiles_dir() == str(tmp_path)


def test_client_uses_execution_project_location_and_query_limits() -> None:
    credentials = MagicMock()
    with (
        patch(
            "dbt_dry_run.adapter.bigquery.create_credentials",
            return_value=(credentials, None),
        ),
        patch("dbt_dry_run.adapter.bigquery.Client") as client,
    ):
        create_bigquery_client(
            output(
                execution_project="billing-project",
                maximum_bytes_billed=1234,
                priority="batch",
                reservation="projects/p/locations/EU/reservations/r",
                job_execution_timeout_seconds=30,
                api_endpoint="https://bigquery.example.com",
            )
        )
    kwargs = client.call_args.kwargs
    assert kwargs["project"] == "billing-project"
    assert kwargs["credentials"] is credentials
    assert kwargs["location"] == "EU"
    assert kwargs["client_options"] == {"api_endpoint": "https://bigquery.example.com"}
    config = kwargs["default_query_job_config"]
    assert config.maximum_bytes_billed == 1234
    assert config.priority == "BATCH"
    assert config.job_timeout_ms == "30000"


@pytest.mark.parametrize("threads_override, expected_threads", [(None, 6), (9, 9)])
def test_project_service_uses_selected_profile(
    tmp_path: Path, threads_override: int | None, expected_threads: int
) -> None:
    (tmp_path / "dbt_project.yml").write_text("profile: default\n")
    (tmp_path / "profiles.yml").write_text(
        "default:\n  target: dev\n  outputs:\n    dev:\n"
        "      type: bigquery\n      method: oauth\n      project: data-project\n"
        "      schema: analytics\n      threads: 6\n"
        "      job_creation_timeout_seconds: 12\n"
    )
    client = MagicMock()
    with patch(
        "dbt_dry_run.adapter.service.create_bigquery_client", return_value=client
    ) as factory:
        service = ProjectService(
            DbtArgs(
                project_dir=str(tmp_path),
                profiles_dir=str(tmp_path),
                threads=threads_override,
            )
        )
    assert service.get_client() is client
    assert service.job_creation_timeout_seconds == 12
    assert service.threads == expected_threads
    assert factory.call_args.args[0].project == "data-project"


def test_query_passes_profile_creation_timeout() -> None:
    project = MagicMock()
    project.job_creation_timeout_seconds = 12
    project.get_client.return_value.query.return_value.schema = []
    BigQuerySQLRunner(cast(ProjectService, project)).query("select 1")
    assert project.get_client.return_value.query.call_args.kwargs["timeout"] == 12
