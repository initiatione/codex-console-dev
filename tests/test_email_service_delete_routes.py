import asyncio
from contextlib import contextmanager
from pathlib import Path

import pytest
from fastapi import HTTPException

from src.database.models import Base, EmailService, RegistrationTask
from src.database.session import DatabaseSessionManager
from src.web.routes import email as email_routes


TEST_SERVICE_CONFIG = {
    "base_url": "https://api.duckmail.test",
    "default_domain": "duckmail.sbs",
    "api_key": "dk_test_key",
}


def _build_fake_get_db(manager: DatabaseSessionManager):
    @contextmanager
    def fake_get_db():
        session = manager.SessionLocal()
        try:
            yield session
        finally:
            session.close()

    return fake_get_db


def test_delete_email_service_rejects_when_referenced(monkeypatch):
    runtime_dir = Path("tests_runtime")
    runtime_dir.mkdir(exist_ok=True)
    db_path = runtime_dir / "delete_email_service_referenced.db"
    if db_path.exists():
        db_path.unlink()

    manager = DatabaseSessionManager(f"sqlite:///{db_path}")
    Base.metadata.create_all(bind=manager.engine)

    with manager.session_scope() as session:
        service = EmailService(
            service_type="duck_mail",
            name="DuckMail 主服务",
            config=TEST_SERVICE_CONFIG,
            enabled=True,
            priority=0,
        )
        session.add(service)
        session.flush()
        service_id = service.id
        session.add(
            RegistrationTask(
                task_uuid="task-delete-email-service-001",
                email_service_id=service_id,
                status="completed",
            )
        )

    monkeypatch.setattr(email_routes, "get_db", _build_fake_get_db(manager))

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(email_routes.delete_email_service(service_id))

    assert exc_info.value.status_code == 409
    assert "已被 1 个注册任务引用" in exc_info.value.detail
    assert "改为禁用" in exc_info.value.detail

    with manager.session_scope() as session:
        assert session.query(EmailService).count() == 1


def test_delete_email_service_succeeds_when_unreferenced(monkeypatch):
    runtime_dir = Path("tests_runtime")
    runtime_dir.mkdir(exist_ok=True)
    db_path = runtime_dir / "delete_email_service_unreferenced.db"
    if db_path.exists():
        db_path.unlink()

    manager = DatabaseSessionManager(f"sqlite:///{db_path}")
    Base.metadata.create_all(bind=manager.engine)

    with manager.session_scope() as session:
        service = EmailService(
            service_type="duck_mail",
            name="DuckMail 可删除服务",
            config=TEST_SERVICE_CONFIG,
            enabled=True,
            priority=0,
        )
        session.add(service)
        session.flush()
        service_id = service.id
        service_name = service.name

    monkeypatch.setattr(email_routes, "get_db", _build_fake_get_db(manager))

    result = asyncio.run(email_routes.delete_email_service(service_id))

    assert result == {"success": True, "message": f"服务 {service_name} 已删除"}

    with manager.session_scope() as session:
        assert session.query(EmailService).count() == 0