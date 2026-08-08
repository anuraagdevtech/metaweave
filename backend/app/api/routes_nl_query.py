from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.core.db import get_db
from app.nl_query.tools import answer_question

router = APIRouter(prefix="/nl-query", tags=["nl-query"])


class NLQueryRequest(BaseModel):
    question: str


@router.post("")
def nl_query(payload: NLQueryRequest, db: Session = Depends(get_db)) -> dict:
    return answer_question(db, payload.question)
