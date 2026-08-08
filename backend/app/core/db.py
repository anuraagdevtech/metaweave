"""SQLAlchemy engine/session setup, portable across SQLite and Postgres."""
from __future__ import annotations

from collections.abc import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from app.core.config import settings

_connect_args = {"check_same_thread": False} if settings.is_sqlite else {}

engine = create_engine(settings.database_url, connect_args=_connect_args, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


class Base(DeclarativeBase):
    """Shared declarative base for all ORM models."""


def get_db() -> Generator[Session, None, None]:
    """FastAPI dependency yielding a request-scoped DB session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db() -> None:
    """Create all tables. Used for SQLite quick-start / tests.

    Production Postgres schema is owned by Alembic migrations
    (see backend/migrations) — this is not a substitute for them.
    """
    from app.models import metadata  # noqa: F401  (register models on Base)

    Base.metadata.create_all(bind=engine)
