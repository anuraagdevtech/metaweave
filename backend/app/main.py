"""MetaWeave API entrypoint.

Local quick start (SQLite, zero setup):
    uvicorn app.main:app --reload

Production: point DATABASE_URL at Postgres and apply migrations first:
    alembic upgrade head
"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api import (
    routes_blast_radius,
    routes_jobs,
    routes_lineage,
    routes_nl_query,
    routes_workflows,
)
from app.core.config import settings
from app.core.db import init_db


@asynccontextmanager
async def lifespan(app: FastAPI):
    if settings.is_sqlite:
        # Convenience for local dev/tests only — Postgres schema is owned by Alembic.
        init_db()
    yield


app = FastAPI(
    title="MetaWeave API",
    description="Data pipeline tracking, lineage, blast radius/RCA, and NL query metadata platform.",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(routes_workflows.router)
app.include_router(routes_jobs.router)
app.include_router(routes_lineage.router)
app.include_router(routes_blast_radius.router)
app.include_router(routes_nl_query.router)


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "environment": settings.environment}
