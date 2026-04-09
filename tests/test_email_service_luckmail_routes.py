import importlib.util
import sys
import types
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class _DummyRouter:
    def get(self, *args, **kwargs):
        return lambda func: func

    def post(self, *args, **kwargs):
        return lambda func: func

    def put(self, *args, **kwargs):
        return lambda func: func

    def delete(self, *args, **kwargs):
        return lambda func: func

    def patch(self, *args, **kwargs):
        return lambda func: func


def _ensure_package(name, path):
    module = sys.modules.get(name)
    if module is None:
        module = types.ModuleType(name)
        module.__path__ = [str(path)]
        sys.modules[name] = module
    return module


def _load_email_routes_module():
    existing = sys.modules.get("src.web.routes.email")
    if existing is not None:
        return existing

    _ensure_package("src", ROOT / "src")
    _ensure_package("src.web", ROOT / "src" / "web")
    _ensure_package("src.web.routes", ROOT / "src" / "web" / "routes")
    _ensure_package("src.database", ROOT / "src" / "database")
    _ensure_package("src.config", ROOT / "src" / "config")

    fastapi_module = types.ModuleType("fastapi")
    fastapi_module.APIRouter = lambda *args, **kwargs: _DummyRouter()
    fastapi_module.HTTPException = type("HTTPException", (Exception,), {})
    fastapi_module.Query = lambda default=None, **kwargs: default
    sys.modules["fastapi"] = fastapi_module

    pydantic_module = types.ModuleType("pydantic")
    pydantic_module.BaseModel = type("BaseModel", (), {})
    pydantic_module.ConfigDict = dict
    sys.modules["pydantic"] = pydantic_module

    sqlalchemy_module = types.ModuleType("sqlalchemy")
    sqlalchemy_module.func = types.SimpleNamespace(count=lambda *args, **kwargs: None)
    sys.modules["sqlalchemy"] = sqlalchemy_module

    sqlalchemy_exc_module = types.ModuleType("sqlalchemy.exc")
    sqlalchemy_exc_module.IntegrityError = type("IntegrityError", (Exception,), {})
    sys.modules["sqlalchemy.exc"] = sqlalchemy_exc_module

    crud_module = sys.modules.get("src.database.crud")
    if crud_module is None:
        crud_module = types.ModuleType("src.database.crud")
        sys.modules["src.database.crud"] = crud_module
    sys.modules["src.database"].crud = crud_module

    session_module = types.ModuleType("src.database.session")
    session_module.get_db = lambda: (_ for _ in ()).throw(RuntimeError("get_db should not be used in this test"))
    sys.modules["src.database.session"] = session_module

    models_module = types.ModuleType("src.database.models")
    models_module.EmailService = type("EmailService", (), {})
    models_module.Account = type("Account", (), {})
    models_module.RegistrationTask = type("RegistrationTask", (), {})
    sys.modules["src.database.models"] = models_module

    settings_module = types.ModuleType("src.config.settings")
    settings_module.get_settings = lambda: types.SimpleNamespace()
    sys.modules["src.config.settings"] = settings_module

    services_module = types.ModuleType("src.services")
    services_module.EmailServiceFactory = type("EmailServiceFactory", (), {})
    services_module.EmailServiceType = lambda value: value
    sys.modules["src.services"] = services_module

    task_manager_module = types.ModuleType("src.web.task_manager")
    task_manager_module.task_manager = object()
    sys.modules["src.web.task_manager"] = task_manager_module

    spec = importlib.util.spec_from_file_location(
        "src.web.routes.email",
        ROOT / "src" / "web" / "routes" / "email.py",
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules["src.web.routes.email"] = module
    spec.loader.exec_module(module)
    return module


email_routes = _load_email_routes_module()


def _run_sync_coro(coro):
    try:
        coro.send(None)
    except StopIteration as exc:
        return exc.value
    raise AssertionError("Coroutine did not complete synchronously")


def test_email_service_types_include_luckmail_compat_fields():
    result = _run_sync_coro(email_routes.get_service_types())
    luckmail_type = next(item for item in result["types"] if item["value"] == "luckmail")

    field_names = [field["name"] for field in luckmail_type["config_fields"]]

    assert "rust_cli_path" in field_names
    assert "sdk_preference" in field_names
    assert "inbox_mode" in field_names
    assert "reuse_existing_purchases" in field_names
    assert "purchase_scan_pages" in field_names
    assert "purchase_scan_page_size" in field_names
    assert "batch_reuse_probe_workers" in field_names
    assert "batch_reuse_probe_limit" in field_names
    assert "batch_reuse_probe_request_timeout_seconds" in field_names
    assert "ensure_purchase_ready" in field_names
    assert "reuse_purchase_candidate_limit" in field_names
    assert "token_alive_timeout" in field_names
    assert "token_alive_request_timeout" in field_names
    assert "token_alive_poll_interval" in field_names
    assert "purchase_ready_retries" in field_names
    assert "fallback_to_order_on_no_stock" in field_names
    assert "token_mail_fallback" in field_names


def test_normalize_email_service_config_coerces_luckmail_values():
    normalized = email_routes.normalize_email_service_config(
        "luckmail",
        {
            "rust_cli_path": " C:\\Users\\tester\\.cargo\\bin\\luckmail-cli.exe ",
            "sdk_preference": " rust ",
            "inbox_mode": " purchase ",
            "reuse_existing_purchases": "false",
            "purchase_scan_pages": "8",
            "purchase_scan_page_size": "120",
            "batch_reuse_probe_workers": "10",
            "batch_reuse_probe_limit": "30",
            "batch_reuse_probe_request_timeout_seconds": "3",
            "ensure_purchase_ready": "true",
            "reuse_purchase_candidate_limit": "2",
            "token_alive_timeout": "15",
            "token_alive_request_timeout": "4",
            "token_alive_poll_interval": "1.5",
            "purchase_ready_retries": "3",
            "fallback_to_order_on_no_stock": "true",
            "token_mail_fallback": "false",
        },
    )

    assert normalized["rust_cli_path"] == "C:\\Users\\tester\\.cargo\\bin\\luckmail-cli.exe"
    assert normalized["sdk_preference"] == "rust"
    assert normalized["inbox_mode"] == "purchase"
    assert normalized["reuse_existing_purchases"] is False
    assert normalized["purchase_scan_pages"] == 8
    assert normalized["purchase_scan_page_size"] == 120
    assert normalized["batch_reuse_probe_workers"] == 10
    assert normalized["batch_reuse_probe_limit"] == 30
    assert normalized["batch_reuse_probe_request_timeout_seconds"] == 3
    assert normalized["ensure_purchase_ready"] is True
    assert normalized["reuse_purchase_candidate_limit"] == 2
    assert normalized["token_alive_timeout"] == 15
    assert normalized["token_alive_request_timeout"] == 4
    assert normalized["token_alive_poll_interval"] == 1.5
    assert normalized["purchase_ready_retries"] == 3
    assert normalized["fallback_to_order_on_no_stock"] is True
    assert normalized["token_mail_fallback"] is False
