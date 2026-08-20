#!/usr/bin/env python3
"""Seed the configured database with the MetaWeave demo dataset.

Usage:
    cd backend
    python scripts/seed.py                 # uses DATABASE_URL / sqlite default
    DATABASE_URL=postgresql+psycopg://... python scripts/seed.py

No-op if the database already has any workflow rows — safe to run repeatedly.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.core.config import settings  # noqa: E402
from app.core.db import SessionLocal, init_db  # noqa: E402
from app.seed_data import SQL_EXAMPLES, seed_demo_data  # noqa: E402


def main() -> None:
    if settings.is_sqlite:
        init_db()

    db = SessionLocal()
    try:
        created = seed_demo_data(db)
        if created:
            print(f"Seeded {created} SQL task examples (of {len(SQL_EXAMPLES)} available) across "
                  "workflows/jobs/tasks/audits/alerts/dependencies.")
        else:
            print("Database already has data — seed skipped.")
    finally:
        db.close()


if __name__ == "__main__":
    main()
