"""Lineage extraction from orchestration definitions and code.

    from app.extract import extract_path
    result = extract_path("path/to/repo")
    result.summary()            # counts at every level
    result.job_edges()          # job -> job
    result.task_edges()         # task -> task
    result.table_edges()        # source table -> target table, via task
    result.column_edges()       # attribute mappings, via task
"""
from app.extract.models import (
    ColumnEdge,
    ExtractedJob,
    ExtractedTask,
    ExtractionResult,
)
from app.extract.runner import extract_path

__all__ = [
    "ColumnEdge",
    "ExtractedJob",
    "ExtractedTask",
    "ExtractionResult",
    "extract_path",
]
