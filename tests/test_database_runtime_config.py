from pathlib import Path
from datetime import timedelta

import asyncio
import pytest

from src.config import settings as settings_module
from src.core.timezone_utils import utcnow_naive
from src.database.models import AppLog, RegistrationTask, Setting

from src.database import session as session_module
from src.web.routes import logs as logs_routes
from src.web.routes import settings as settings_routes


class DummySettings:
    def __init__(self, url: str = "", log_retention_days: int = 14):
        self.database_url = url
        self.log_retention_days = log_retention_days


class DummySession:
    def query(self, *args, **kwargs):
        class Counter:
            def count(self_inner):
                return 0

        return Counter()


class DummyContext:
    def __init__(self):
        self.session = DummySession()

    def __enter__(self):
        return self.session

    def __exit__(self, exc_type, exc, tb):
        return False


def test_get_database_info_reports_sqlite(monkeypatch, tmp_path):
    db_path = tmp_path / "data.db"
    db_path.write_text("test")

    monkeypatch.setattr(settings_routes, "get_settings", lambda: DummySettings(f"sqlite:///{db_path}"))
    monkeypatch.setattr(settings_routes, "get_db", lambda: DummyContext())

    info = asyncio.run(settings_routes.get_database_info())

    assert info["database_backend"] == "sqlite"
    assert info["database_supports_file_backup"] is True
    assert info["connection_pool"]["backend"] == "sqlite"
    assert info["database_size_bytes"] == db_path.stat().st_size


def test_get_database_info_reports_postgres(monkeypatch):
    monkeypatch.setattr(settings_routes, "get_settings", lambda: DummySettings("postgresql://user:pass@localhost:5432/db"))
    monkeypatch.setattr(settings_routes, "get_db", lambda: DummyContext())

    info = asyncio.run(settings_routes.get_database_info())

    assert info["database_backend"] == "postgresql"
    assert info["database_supports_file_backup"] is False
    assert info["database_size_label"].startswith("由数据库服务端")
    assert info["connection_pool"]["backend"] == "postgresql"


def test_get_database_pool_settings_reads_env(monkeypatch):
    monkeypatch.setenv("APP_DB_POOL_SIZE", "24")
    monkeypatch.setenv("APP_DB_MAX_OVERFLOW", "12")
    monkeypatch.setenv("APP_DB_POOL_TIMEOUT", "45")
    monkeypatch.setenv("APP_DB_POOL_RECYCLE", "900")
    monkeypatch.setenv("APP_DB_POOL_USE_LIFO", "false")

    settings = session_module.get_database_pool_settings("postgresql://user:pass@localhost:5432/db")

    assert settings["backend"] == "postgresql"
    assert settings["pool_size"] == 24
    assert settings["max_overflow"] == 12
    assert settings["pool_timeout"] == 45
    assert settings["pool_recycle"] == 900
    assert settings["pool_use_lifo"] is False


def test_backup_database_rejects_postgres(monkeypatch):
    monkeypatch.setattr(settings_routes, "get_settings", lambda: DummySettings("postgresql://user:pass@localhost:5432/db"))

    with pytest.raises(settings_routes.HTTPException) as exc_info:
        asyncio.run(settings_routes.backup_database())

    assert exc_info.value.status_code == 400
    assert "pg_dump" in exc_info.value.detail


def test_log_retention_default_is_14_days():
    assert settings_module.SETTING_DEFINITIONS["log_retention_days"].default_value == 14
    assert settings_module.Settings().log_retention_days == 14


def test_log_stats_returns_runtime_cleanup_defaults(monkeypatch, tmp_path):
    db_path = tmp_path / "logs.db"
    manager = session_module.DatabaseSessionManager(f"sqlite:///{db_path}")
    manager.create_tables()
    now = utcnow_naive()

    with manager.session_scope() as db:
        db.add_all([
            AppLog(level="INFO", logger="tests.logs", module="test", message="ok", created_at=now - timedelta(minutes=1)),
            AppLog(level="ERROR", logger="tests.logs", module="test", message="boom", created_at=now),
        ])

    monkeypatch.setattr(logs_routes, "get_db", manager.session_scope)
    monkeypatch.setattr(logs_routes, "get_settings", lambda: DummySettings(log_retention_days=21))

    result = logs_routes.log_stats()

    assert result["total"] == 2
    assert result["levels"]["INFO"] == 1
    assert result["levels"]["ERROR"] == 1
    assert result["retention_days"] == 21
    assert result["max_rows"] == 50000
    assert result["latest_at"] is not None


def test_cleanup_database_default_strategy_removes_only_stale_tasks(monkeypatch, tmp_path):
    db_path = tmp_path / "cleanup.db"
    manager = session_module.DatabaseSessionManager(f"sqlite:///{db_path}")
    manager.create_tables()
    now = utcnow_naive()

    def add_task(task_uuid: str, status: str, created_days: int, started_days: int | None = None, completed_days: int | None = None):
        with manager.session_scope() as db:
            db.add(
                RegistrationTask(
                    task_uuid=task_uuid,
                    status=status,
                    created_at=now - timedelta(days=created_days),
                    started_at=None if started_days is None else now - timedelta(days=started_days),
                    completed_at=None if completed_days is None else now - timedelta(days=completed_days),
                )
            )

    add_task("completed-old", "completed", created_days=20, completed_days=18)
    add_task("completed-new", "completed", created_days=5, completed_days=4)
    add_task("cancelled-old", "cancelled", created_days=18, completed_days=16)
    add_task("failed-old", "failed", created_days=60, started_days=59, completed_days=58)
    add_task("failed-new", "failed", created_days=12, started_days=11, completed_days=10)
    add_task("pending-old", "pending", created_days=10)
    add_task("pending-new", "pending", created_days=2)
    add_task("running-old", "running", created_days=5, started_days=4)
    add_task("running-new", "running", created_days=1, started_days=1)

    monkeypatch.setattr(settings_routes, "get_db", manager.session_scope)

    result = asyncio.run(settings_routes.cleanup_database())

    assert result["success"] is True
    assert result["deleted_count"] == 5
    assert result["retention_days"] == 14
    assert result["failed_retention_days"] == 45
    assert result["stale_pending_days"] == 7
    assert result["stale_running_days"] == 3

    with manager.session_scope() as db:
        remaining = {row.task_uuid for row in db.query(RegistrationTask).all()}

    assert remaining == {
        "completed-new",
        "failed-new",
        "pending-new",
        "running-new",
    }


def test_init_default_settings_upgrades_legacy_log_retention(monkeypatch, tmp_path):
    db_path = tmp_path / "legacy-settings.db"
    manager = session_module.DatabaseSessionManager(f"sqlite:///{db_path}")
    manager.create_tables()

    with manager.session_scope() as db:
        db.add(
            Setting(
                key="log.retention_days",
                value="30",
                description="日志保留天数",
                category="log",
            )
        )

    monkeypatch.setattr(session_module, "get_db", manager.session_scope)
    settings_module.init_default_settings()

    with manager.session_scope() as db:
        upgraded_value = db.query(Setting).filter(Setting.key == "log.retention_days").one().value

    assert upgraded_value == "14"

