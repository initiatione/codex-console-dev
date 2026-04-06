import asyncio
from contextlib import contextmanager
from pathlib import Path

from src.config.constants import EmailServiceType
from src.database.models import Base, EmailService
from src.database.session import DatabaseSessionManager
from src.services.base import EmailServiceFactory
from src.web.routes import email as email_routes
from src.web.routes import registration as registration_routes


class DummySettings:
    custom_domain_base_url = ""
    custom_domain_api_key = None
    tempmail_enabled = False
    yyds_mail_enabled = False


def test_duckduckgo_cloudmail_service_registered():
    service_type = EmailServiceType("duckduckgo_cloudmail")
    service_class = EmailServiceFactory.get_service_class(service_type)
    assert service_class is not None
    assert service_class.__name__ == "DuckDuckGoCloudMailService"


def test_email_service_types_include_duckduckgo_cloudmail():
    result = asyncio.run(email_routes.get_service_types())
    item = next(entry for entry in result["types"] if entry["value"] == "duckduckgo_cloudmail")

    assert item["label"] == "DuckDuckGo-CloudMail"
    field_names = [field["name"] for field in item["config_fields"]]
    assert "bridge_base_url" in field_names
    assert "bridge_token" in field_names
    assert "forward_to_email" in field_names
    assert "cloudmail_service_id" in field_names


def test_filter_sensitive_config_marks_duckduckgo_cloudmail_secrets():
    filtered = email_routes.filter_sensitive_config({
        "bridge_base_url": "http://127.0.0.1:1456",
        "bridge_token": "bridge-secret",
        "forward_to_email": "relay@example.com",
        "cloudmail_admin_password": "cm-secret",
    })

    assert filtered["bridge_base_url"] == "http://127.0.0.1:1456"
    assert filtered["forward_to_email"] == "relay@example.com"
    assert filtered["has_bridge_token"] is True
    assert filtered["has_cloudmail_admin_password"] is True
    assert "bridge_token" not in filtered
    assert "cloudmail_admin_password" not in filtered


def test_resolve_runtime_email_service_config_merges_linked_cloudmail_service():
    runtime_dir = Path("tests_runtime")
    runtime_dir.mkdir(exist_ok=True)
    db_path = runtime_dir / "duckduckgo_cloudmail_routes.db"
    if db_path.exists():
        db_path.unlink()

    manager = DatabaseSessionManager(f"sqlite:///{db_path}")
    Base.metadata.create_all(bind=manager.engine)

    with manager.session_scope() as session:
        session.add(
            EmailService(
                service_type="cloudmail",
                name="CloudMail 主服务",
                config={
                    "base_url": "https://cloudmail.test",
                    "admin_email": "admin@cloudmail.test",
                    "admin_password": "cm-secret",
                    "domain": "cloudmail.test",
                },
                enabled=True,
                priority=0,
            )
        )
        session.commit()
        cloudmail_id = session.query(EmailService).filter(EmailService.service_type == "cloudmail").first().id

        resolved = email_routes.resolve_runtime_email_service_config(
            session,
            "duckduckgo_cloudmail",
            {
                "bridge_base_url": "http://127.0.0.1:1456",
                "forward_to_email": "relay@example.com",
                "cloudmail_service_id": cloudmail_id,
            },
        )

    assert resolved["bridge_base_url"] == "http://127.0.0.1:1456"
    assert resolved["forward_to_email"] == "relay@example.com"
    assert resolved["cloudmail_base_url"] == "https://cloudmail.test"
    assert resolved["cloudmail_admin_email"] == "admin@cloudmail.test"
    assert resolved["cloudmail_admin_password"] == "cm-secret"


def test_registration_available_services_include_duckduckgo_cloudmail(monkeypatch):
    runtime_dir = Path("tests_runtime")
    runtime_dir.mkdir(exist_ok=True)
    db_path = runtime_dir / "duckduckgo_cloudmail_available_services.db"
    if db_path.exists():
        db_path.unlink()

    manager = DatabaseSessionManager(f"sqlite:///{db_path}")
    Base.metadata.create_all(bind=manager.engine)

    with manager.session_scope() as session:
        session.add(
            EmailService(
                service_type="duckduckgo_cloudmail",
                name="Duck 池桥接服务",
                config={
                    "bridge_base_url": "http://127.0.0.1:1456",
                    "forward_to_email": "relay@example.com",
                    "cloudmail_service_id": 7,
                },
                enabled=True,
                priority=2,
            )
        )

    @contextmanager
    def fake_get_db():
        session = manager.SessionLocal()
        try:
            yield session
        finally:
            session.close()

    monkeypatch.setattr(registration_routes, "get_db", fake_get_db)

    import src.config.settings as settings_module

    monkeypatch.setattr(settings_module, "get_settings", lambda: DummySettings())

    result = asyncio.run(registration_routes.get_available_email_services())

    assert result["duckduckgo_cloudmail"]["available"] is True
    assert result["duckduckgo_cloudmail"]["count"] == 1
    assert result["duckduckgo_cloudmail"]["services"][0]["name"] == "Duck 池桥接服务"
    assert result["duckduckgo_cloudmail"]["services"][0]["type"] == "duckduckgo_cloudmail"
    assert result["duckduckgo_cloudmail"]["services"][0]["forward_to_email"] == "relay@example.com"
    assert result["duckduckgo_cloudmail"]["services"][0]["cloudmail_service_id"] == 7
