"""Create the database schema for the database the API is configured to use.

Reads DATABASE_URL from the environment / .env exactly like the API does, so
the schema is created in the same database the server will open.

  SQLite      → creates any missing tables (safe to re-run; never drops data).
  PostgreSQL  → use Alembic migrations instead: `alembic upgrade head`.
"""
from __future__ import annotations

import sys

from sqlalchemy import inspect

from app.core.config import get_settings
from app.persistence.database import describe_database_url, get_sync_engine
from app.persistence.models import Base


def main() -> int:
    settings = get_settings()
    url = settings.database_url
    print(f"Database: {describe_database_url(url)}")

    if not url.startswith("sqlite"):
        print("This is not a SQLite database. Apply the migrations instead:\n  alembic upgrade head")
        return 1

    engine = get_sync_engine()
    before = set(inspect(engine).get_table_names())
    Base.metadata.create_all(bind=engine)
    created = sorted(set(inspect(engine).get_table_names()) - before)
    print(f"Created tables: {', '.join(created)}" if created else "Schema already up to date.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
