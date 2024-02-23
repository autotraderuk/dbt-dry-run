from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from threading import Lock
from typing import Dict, List, Optional, Set

from dbt_dry_run.models.manifest import Node
from dbt_dry_run.models.table import Table


class DryRunStatus(str, Enum):
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    SKIPPED = "SKIPPED"


class LintingStatus(str, Enum):
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    SKIPPED = "SKIPPED"


@dataclass(frozen=True)
class LintingError:
    rule: str
    message: str


@dataclass(frozen=True)
class DryRunResult:
    node: Node
    table: Optional[Table]
    status: DryRunStatus
    exception: Optional[Exception]
    linting_status: LintingStatus = LintingStatus.SKIPPED
    linting_errors: List[LintingError] = field(default_factory=lambda: [])

    def replace_table(self, table: Table) -> "DryRunResult":
        return DryRunResult(
            node=self.node, table=table, status=self.status, exception=self.exception
        )

    def with_linting_errors(self, linting_errors: List[LintingError]) -> "DryRunResult":
        if linting_errors:
            linting_status = LintingStatus.FAILURE
        else:
            linting_status = LintingStatus.SUCCESS
        return DryRunResult(
            node=self.node,
            table=self.table,
            status=self.status,
            exception=self.exception,
            linting_errors=linting_errors,
            linting_status=linting_status,
        )


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
