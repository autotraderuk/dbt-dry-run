import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

import google.auth
from google.auth.credentials import Credentials, CredentialsWithQuotaProject
from google.auth.identity_pool import Credentials as IdentityPoolCredentials
from google.auth.identity_pool import SubjectTokenSupplier
from google.cloud.bigquery import Client, QueryJobConfig
from google.oauth2.service_account import Credentials as ServiceAccountCredentials

from dbt_dry_run.models.profile import BigQueryConnectionMethod, Output, TokenEndpoint


class EntraTokenSupplier(SubjectTokenSupplier):
    def __init__(self, endpoint: TokenEndpoint):
        self._endpoint = endpoint
        self._token: Optional[str] = None
        self._expires_at: Optional[datetime] = None

    def get_subject_token(self, context: Any, request: Any) -> str:
        if (
            self._token
            and self._expires_at
            and datetime.now(timezone.utc) < self._expires_at
        ):
            return self._token
        response = request(
            url=self._endpoint.request_url,
            method="POST",
            headers={
                "accept": "application/json",
                "content-type": "application/x-www-form-urlencoded",
            },
            body=self._endpoint.request_data.encode("utf-8"),
        )
        if response.status != 200:
            raise ValueError(f"Identity provider returned HTTP {response.status}")
        token_data = json.loads(response.data)
        if not isinstance(token_data, dict) or not token_data.get("access_token"):
            raise ValueError("Identity provider did not return an access_token")
        self._token = token_data["access_token"]
        expires_in = int(token_data.get("expires_in", 3600))
        self._expires_at = datetime.now(timezone.utc) + timedelta(
            seconds=max(0, expires_in - 300)
        )
        return self._token


def create_credentials(output: Output) -> Tuple[Credentials, Optional[str]]:
    scopes = output.scopes or None
    method = output.method
    discovered_project: Optional[str] = None
    if method == BigQueryConnectionMethod.OAUTH:
        credentials, discovered_project = google.auth.default(scopes=scopes)
    elif method == BigQueryConnectionMethod.SERVICE_ACCOUNT:
        assert output.keyfile is not None
        credentials = ServiceAccountCredentials.from_service_account_file(
            str(output.keyfile), scopes=scopes
        )
    elif method == BigQueryConnectionMethod.EXTERNAL_OAUTH_WIF:
        assert output.token_endpoint is not None
        assert output.workload_pool_provider_path is not None
        credentials = IdentityPoolCredentials(
            audience=output.workload_pool_provider_path,
            subject_token_type="urn:ietf:params:oauth:token-type:jwt",
            token_url="https://sts.googleapis.com/v1/token",
            subject_token_supplier=EntraTokenSupplier(output.token_endpoint),
            service_account_impersonation_url=output.service_account_impersonation_url,
        ).with_scopes(scopes)
    else:
        raise ValueError(f"Unsupported BigQuery authentication method: {method}")

    if output.quota_project:
        if not isinstance(credentials, CredentialsWithQuotaProject):
            raise ValueError("Selected Google credentials do not support quota_project")
        credentials = credentials.with_quota_project(output.quota_project)
    credential_project = getattr(credentials, "project_id", None)
    return credentials, discovered_project or (
        credential_project if isinstance(credential_project, str) else None
    )


def create_bigquery_client(output: Output) -> Client:
    credentials, discovered_project = create_credentials(output)
    project = output.execution_project or output.project or discovered_project
    if not project:
        raise ValueError(
            "BigQuery profile must specify a project or resolve one through credentials"
        )
    job_options: Dict[str, Any] = {}
    if output.priority:
        job_options["priority"] = output.priority.upper()
    if output.maximum_bytes_billed is not None:
        job_options["maximum_bytes_billed"] = output.maximum_bytes_billed
    if output.reservation:
        job_options["reservation"] = output.reservation
    if output.job_execution_timeout_seconds is not None:
        job_options["job_timeout_ms"] = output.job_execution_timeout_seconds * 1000
    options: Dict[str, Any] = {}
    if output.api_endpoint:
        options["client_options"] = {"api_endpoint": output.api_endpoint}
    return Client(
        project=project,
        credentials=credentials,
        location=output.location,
        default_query_job_config=QueryJobConfig(**job_options),
        **options,
    )
