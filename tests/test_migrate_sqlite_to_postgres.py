from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest
import sqlalchemy as sa

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "migrate_sqlite_to_postgres.py"
SPEC = importlib.util.spec_from_file_location("migrate_sqlite_to_postgres", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
migration = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = migration
SPEC.loader.exec_module(migration)


def test_normalize_sqlite_url_accepts_file_path():
    assert migration.normalize_sqlite_url("data/database.db") == "sqlite:///data/database.db"


def test_normalize_postgres_url_accepts_postgres_scheme():
    actual = migration.normalize_postgres_url("postgres://user:pass@localhost:5432/demo")
    assert actual == "postgresql+psycopg://user:pass@localhost:5432/demo"


def test_normalize_postgres_url_rejects_non_postgres_target():
    with pytest.raises(migration.MigrationError):
        migration.normalize_postgres_url("sqlite:///data/database.db")


def test_chunked_rows_splits_batches():
    rows = [{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}, {"id": 5}]
    chunks = list(migration.chunked_rows(rows, 2))
    assert chunks == [
        [{"id": 1}, {"id": 2}],
        [{"id": 3}, {"id": 4}],
        [{"id": 5}],
    ]


def test_build_table_plan_uses_common_columns_and_model_order():
    source = sa.MetaData()
    target = sa.MetaData()

    sa.Table(
        "accounts",
        source,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("email", sa.String(255)),
        sa.Column("status", sa.String(20)),
    )
    sa.Table(
        "settings",
        source,
        sa.Column("key", sa.String(100), primary_key=True),
        sa.Column("value", sa.Text),
    )

    sa.Table(
        "accounts",
        target,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("email", sa.String(255)),
        sa.Column("status", sa.String(20)),
        sa.Column("created_at", sa.DateTime),
    )
    sa.Table(
        "settings",
        target,
        sa.Column("key", sa.String(100), primary_key=True),
        sa.Column("value", sa.Text),
        sa.Column("updated_at", sa.DateTime),
    )

    plan = migration.build_table_plan(source, target)

    assert [item.name for item in plan[:2]] == ["accounts", "settings"]
    assert plan[0].columns == ("id", "email", "status")
    assert plan[1].columns == ("key", "value")