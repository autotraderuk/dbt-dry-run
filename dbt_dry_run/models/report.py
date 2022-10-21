from typing import List, Optional

from pydantic import Field
from pydantic.main import BaseModel

from ..results import LintingStatus
from .table import Table


class ReportLintingError(BaseModel):
    rule: str
    message: str


class ReportNode(BaseModel):
    unique_id: str
    success: bool
    error_message: Optional[str]
    table: Optional[Table]
    linting_status: LintingStatus
    linting_errors: List[ReportLintingError]


class Report(BaseModel):
    success: bool
    execution_time: float
    node_count: int = Field(..., ge=0)
    failure_count: int = Field(..., ge=0)
    failed_node_ids: List[str] = []
    nodes: List[ReportNode]
