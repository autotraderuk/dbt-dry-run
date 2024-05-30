import os
from argparse import Namespace
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

from dbt.adapters.base import BaseAdapter
from dbt.adapters.contracts.connection import Connection
from dbt.adapters.factory import get_adapter, register_adapter, reset_adapters
from dbt.config import RuntimeConfig
from dbt.flags import set_from_args
from dbt.mp_context import get_mp_context
from dbt_common.context import set_invocation_context

from dbt_dry_run.adapter.utils import default_profiles_dir
from dbt_dry_run.models import Manifest


@dataclass(frozen=True)
class DbtArgs:
    profiles_dir: str = field(default_factory=default_profiles_dir)
    project_dir: str = os.getcwd()
    profile: Optional[str] = None
    target: Optional[str] = None
    target_path: Optional[str] = None
    vars: Dict[str, Any] = field(default_factory=dict)
    threads: Optional[int] = None

    dependencies: List[str] = field(default_factory=list)

    def to_namespace(self) -> Namespace:
        self_as_dict = asdict(self)
        # self_as_dict["vars"] = json.loads(self_as_dict["vars"])
        return Namespace(**self_as_dict)


def set_dbt_args(args: DbtArgs) -> None:
    set_from_args(args.to_namespace(), args)


class ProjectService:
    def __init__(self, args: DbtArgs):
        self._args = args
        set_from_args(self._args.to_namespace(), self._args)
        set_invocation_context(os.environ)
        self._config = RuntimeConfig.from_args(self._args)
        reset_adapters()
        register_adapter(self._config, get_mp_context())
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
        return self._config.threads

    @property
    def adapter(self) -> BaseAdapter:
        return self._adapter
