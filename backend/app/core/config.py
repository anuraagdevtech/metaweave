"""Runtime configuration for the MetaWeave API.

All settings are sourced from environment variables so the same codebase runs
unmodified against local SQLite (tests / quick start) and production Postgres.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Settings:
    # Defaults to a local SQLite file so `uvicorn app.main:app` works with zero
    # setup. Point DATABASE_URL at Postgres for production, e.g.:
    #   postgresql+psycopg://metaweave:metaweave@localhost:5432/metaweave
    database_url: str = field(
        default_factory=lambda: os.environ.get(
            "DATABASE_URL", "sqlite:///./metaweave.db"
        )
    )
    anthropic_api_key: str | None = field(
        default_factory=lambda: os.environ.get("ANTHROPIC_API_KEY")
    )
    anthropic_model: str = field(
        default_factory=lambda: os.environ.get(
            "ANTHROPIC_MODEL", "claude-sonnet-5"
        )
    )
    environment: str = field(
        default_factory=lambda: os.environ.get("METAWEAVE_ENV", "development")
    )

    @property
    def is_sqlite(self) -> bool:
        return self.database_url.startswith("sqlite")


settings = Settings()
