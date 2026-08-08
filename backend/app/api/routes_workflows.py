from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.db import get_db
from app.models.metadata import WorkflowDefinition
from app.schemas.metadata import WorkflowCreate, WorkflowOut

router = APIRouter(prefix="/workflows", tags=["workflows"])


@router.get("", response_model=list[WorkflowOut])
def list_workflows(db: Session = Depends(get_db)) -> list[WorkflowDefinition]:
    return list(db.execute(select(WorkflowDefinition)).scalars().all())


@router.post("", response_model=WorkflowOut, status_code=201)
def create_workflow(payload: WorkflowCreate, db: Session = Depends(get_db)) -> WorkflowDefinition:
    workflow = WorkflowDefinition(**payload.model_dump())
    db.add(workflow)
    db.commit()
    db.refresh(workflow)
    return workflow


@router.get("/{workflow_id}", response_model=WorkflowOut)
def get_workflow(workflow_id: str, db: Session = Depends(get_db)) -> WorkflowDefinition:
    workflow = db.get(WorkflowDefinition, workflow_id)
    if workflow is None:
        raise HTTPException(404, f"Workflow {workflow_id} not found")
    return workflow
