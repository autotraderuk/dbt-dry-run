import os
from argparse import Namespace
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

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
    threads: int = 8

    dependencies: List[str] = field(default_factory=list)

    def to_namespace(self) -> Namespace:
        self_as_dict = asdict(self)
        # self_as_dict["vars"] = json.loads(self_as_dict["vars"])
        return Namespace(**self_as_dict)


class ProjectService:
    def __init__(self, args: DbtArgs):
        self._args = args
        self._client = Client()

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
        return self._args.threads

    def get_client(self) -> Client:
        return self._client
