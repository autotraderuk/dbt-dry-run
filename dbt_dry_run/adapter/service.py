import os
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from dbt_dry_run.adapter.bigquery import create_bigquery_client
from dbt_dry_run.adapter.profile import load_selected_output
from dbt_dry_run.adapter.utils import default_profiles_dir
from dbt_dry_run.models import Manifest
from google.cloud.bigquery import Client


@dataclass(frozen=True)
class DbtArgs:
    profiles_dir: str = field(default_factory=default_profiles_dir)
    project_dir: str = os.getcwd()
    profile: Optional[str] = None
    target: Optional[str] = None
    target_path: str = "target"
    vars: Dict[str, Any] = field(default_factory=dict)
    threads: Optional[int] = None


class ProjectService:
    def __init__(self, args: DbtArgs):
        self._args = args
        self._output = load_selected_output(
            args.project_dir,
            args.profiles_dir,
            profile_override=args.profile,
            target_override=args.target,
            cli_vars=args.vars,
        )
        self._client = create_bigquery_client(self._output)

    @property
    def manifest_filepath(self) -> str:
        return os.path.join(
            self._args.project_dir, self._args.target_path, "manifest.json"
        )

    def get_dbt_manifest(self) -> Manifest:
        manifest = Manifest.from_filepath(self.manifest_filepath)

        return manifest

    @property
    def threads(self) -> int:
        return (
            self._args.threads
            if self._args.threads is not None
            else self._output.threads
        )

    def get_client(self) -> Client:
        return self._client

    @property
    def job_creation_timeout_seconds(self) -> Optional[int]:
        return self._output.job_creation_timeout_seconds
