import os
from dataclasses import dataclass
from typing import Optional

from dbt.adapters.factory import get_adapter, register_adapter, reset_adapters
from dbt.config import RuntimeConfig
from dbt.contracts.connection import Connection
from dbt.flags import DEFAULT_PROFILES_DIR, set_from_args

from dbt_dry_run.models import Manifest


@dataclass(frozen=True)
class DbtArgs:
    profiles_dir: str = DEFAULT_PROFILES_DIR
    project_dir: str = os.getcwd()
    profile: Optional[str] = None
    target: Optional[str] = None
    vars: str = "{}"


def set_dbt_args(args: DbtArgs) -> None:
    set_from_args(args, args)


class ProjectService:
    def __init__(self, args: DbtArgs):
        self._args = args
        set_dbt_args(self._args)
        dbt_project, dbt_profile = RuntimeConfig.collect_parts(self._args)
        self._profile = dbt_profile
        self._config = RuntimeConfig.from_parts(dbt_project, dbt_profile, self._args)
        reset_adapters()
        register_adapter(self._config)
        self._adapter = get_adapter(self._config)

    def get_connection(self) -> Connection:
        connection = self._adapter.connections.set_connection_name("dbt-dry-run")
        return connection

    @property
    def manifest_filepath(self) -> str:
        return os.path.join(
            self._config.project_root, self._config.target_path, "manifest.json"
        )

    def get_dbt_manifest(self) -> Manifest:
        manifest = Manifest.from_filepath(self.manifest_filepath)

        return manifest

    @property
    def threads(self) -> int:
        return self._profile.threads
