from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.db import get_db
from app.models.metadata import JobAlert, JobAudit, JobDefinition, JobDependency, TaskDefinition
from app.schemas.metadata import (
    JobAlertCreate,
    JobAlertOut,
    JobAuditCreate,
    JobAuditOut,
    JobCreate,
    JobDependencyCreate,
    JobDependencyOut,
    JobOut,
    TaskCreate,
    TaskOut,
)

router = APIRouter(tags=["jobs"])


@router.get("/jobs", response_model=list[JobOut])
def list_jobs(workflow_id: str | None = None, db: Session = Depends(get_db)) -> list[JobDefinition]:
    query = select(JobDefinition)
    if workflow_id:
        query = query.where(JobDefinition.workflow_id == workflow_id)
    return list(db.execute(query).scalars().all())


@router.post("/jobs", response_model=JobOut, status_code=201)
def create_job(payload: JobCreate, db: Session = Depends(get_db)) -> JobDefinition:
    job = JobDefinition(**payload.model_dump())
    db.add(job)
    db.commit()
    db.refresh(job)
    return job


@router.get("/jobs/{job_id}", response_model=JobOut)
def get_job(job_id: str, db: Session = Depends(get_db)) -> JobDefinition:
    job = db.get(JobDefinition, job_id)
    if job is None:
        raise HTTPException(404, f"Job {job_id} not found")
    return job


@router.get("/jobs/{job_id}/tasks", response_model=list[TaskOut])
def list_tasks(job_id: str, db: Session = Depends(get_db)) -> list[TaskDefinition]:
    query = select(TaskDefinition).where(TaskDefinition.job_id == job_id).order_by(TaskDefinition.task_order)
    return list(db.execute(query).scalars().all())


@router.post("/tasks", response_model=TaskOut, status_code=201)
def create_task(payload: TaskCreate, db: Session = Depends(get_db)) -> TaskDefinition:
    task = TaskDefinition(**payload.model_dump())
    db.add(task)
    db.commit()
    db.refresh(task)
    return task


@router.get("/jobs/{job_id}/audits", response_model=list[JobAuditOut])
def list_audits(job_id: str, limit: int = 50, db: Session = Depends(get_db)) -> list[JobAudit]:
    query = (
        select(JobAudit)
        .where(JobAudit.job_id == job_id)
        .order_by(JobAudit.started_at.desc())
        .limit(limit)
    )
    return list(db.execute(query).scalars().all())


@router.post("/audits", response_model=JobAuditOut, status_code=201)
def create_audit(payload: JobAuditCreate, db: Session = Depends(get_db)) -> JobAudit:
    audit = JobAudit(**payload.model_dump())
    db.add(audit)
    db.commit()
    db.refresh(audit)
    return audit


@router.post("/dependencies", response_model=JobDependencyOut, status_code=201)
def create_dependency(payload: JobDependencyCreate, db: Session = Depends(get_db)) -> JobDependency:
    dependency = JobDependency(**payload.model_dump())
    db.add(dependency)
    db.commit()
    db.refresh(dependency)
    return dependency


@router.get("/tasks/{task_id}/dependencies", response_model=list[JobDependencyOut])
def list_dependencies(task_id: str, db: Session = Depends(get_db)) -> list[JobDependency]:
    query = select(JobDependency).where(JobDependency.task_id == task_id)
    return list(db.execute(query).scalars().all())


@router.post("/alerts", response_model=JobAlertOut, status_code=201)
def create_alert(payload: JobAlertCreate, db: Session = Depends(get_db)) -> JobAlert:
    alert = JobAlert(**payload.model_dump())
    db.add(alert)
    db.commit()
    db.refresh(alert)
    return alert


@router.get("/jobs/{job_id}/alerts", response_model=list[JobAlertOut])
def list_alerts(job_id: str, db: Session = Depends(get_db)) -> list[JobAlert]:
    query = select(JobAlert).where(JobAlert.job_id == job_id)
    return list(db.execute(query).scalars().all())
