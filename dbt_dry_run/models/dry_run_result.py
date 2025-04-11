from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional

from .manifest import Node
from .table import Table


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
            node=self.node,
            table=table,
            status=self.status,
            exception=self.exception,
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
