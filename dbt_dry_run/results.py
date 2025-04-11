from datetime import datetime
from threading import Lock
from typing import Dict, List, Optional, Set

from dbt_dry_run.models.dry_run_result import DryRunResult


class Results:
    def __init__(self) -> None:
        self._results: Dict[str, DryRunResult] = {}
        self._lock = Lock()
        self._start_time = datetime.utcnow()
        self._end_time: Optional[datetime] = None

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

    def finish(self) -> None:
        self._end_time = datetime.utcnow()

    @property
    def execution_time_in_seconds(self) -> Optional[float]:
        if self._end_time:
            return (self._end_time - self._start_time).seconds
        return None
