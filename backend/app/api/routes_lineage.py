from __future__ import annotations

from dataclasses import asdict

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.lineage.pyspark_parser import parse_pyspark_lineage
from app.lineage.sql_parser import UnsupportedStatementError, parse_sql_script

router = APIRouter(prefix="/lineage", tags=["lineage"])


class SqlLineageRequest(BaseModel):
    sql: str
    dialect: str = "postgres"


class PySparkLineageRequest(BaseModel):
    code: str


@router.post("/parse-sql")
def parse_sql(payload: SqlLineageRequest) -> list[dict]:
    try:
        results = parse_sql_script(payload.sql, dialect=payload.dialect)
    except UnsupportedStatementError as exc:
        raise HTTPException(400, str(exc)) from exc
    return [asdict(r) for r in results]


@router.post("/parse-pyspark")
def parse_pyspark(payload: PySparkLineageRequest) -> list[dict]:
    try:
        results = parse_pyspark_lineage(payload.code)
    except SyntaxError as exc:
        raise HTTPException(400, f"Invalid Python: {exc}") from exc
    return [asdict(r) for r in results]
