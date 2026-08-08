from __future__ import annotations

from dataclasses import asdict

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.core.db import get_db
from app.lineage.graph import build_dependency_graph

router = APIRouter(tags=["blast-radius"])


@router.get("/blast-radius/{node_id}")
def blast_radius(node_id: str, max_depth: int = Query(10, ge=1, le=100), db: Session = Depends(get_db)) -> dict:
    graph = build_dependency_graph(db)
    nodes = graph.blast_radius(node_id, max_depth=max_depth)
    return {"root": node_id, "impacted_nodes": [asdict(n) for n in nodes]}


@router.get("/rca/{node_id}")
def rca(node_id: str, max_depth: int = Query(10, ge=1, le=100), db: Session = Depends(get_db)) -> dict:
    graph = build_dependency_graph(db)
    nodes = graph.rca(node_id, max_depth=max_depth)
    return {"root": node_id, "root_cause_candidates": [asdict(n) for n in nodes]}
