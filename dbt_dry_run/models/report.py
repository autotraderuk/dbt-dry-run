from typing import List, Optional

from pydantic import Field
from pydantic.main import BaseModel

from .table import Table


class ReportNode(BaseModel):
    unique_id: str
    success: bool
    error_message: Optional[str]
    table: Optional[Table]


class Report(BaseModel):
    success: bool
    node_count: int = Field(..., ge=0)
    failure_count: int = Field(..., ge=0)
    failed_node_ids: List[str] = []
    nodes: List[ReportNode]
