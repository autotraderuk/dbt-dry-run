from threading import Lock
from typing import Dict, List, Set

from dbt_dry_run.models import DryRunResult


class Results:
    def __init__(self) -> None:
        self._results: Dict[str, DryRunResult] = {}
        self._lock = Lock()

    def add_result(self, node_key: str, result: DryRunResult) -> None:
        with self._lock:
            self._results[node_key] = result

    def get_result(self, node_key: str) -> DryRunResult:
        with self._lock:
            return self._results[node_key]

    def keys(self) -> Set[str]:
        with self._lock:
            return set(self._results.keys())

    def values(self) -> List[DryRunResult]:
        with self._lock:
            return list(self._results.values())
