import logging
import importlib.util
import sys
import types
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[1]


def _ensure_package(name, path):
    module = sys.modules.get(name)
    if module is None:
        module = types.ModuleType(name)
        module.__path__ = [str(path)]
        sys.modules[name] = module
    return module


def _load_module(name, path):
    existing = sys.modules.get(name)
    if existing is not None:
        return existing
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


_ensure_package("src", ROOT / "src")
_ensure_package("src.config", ROOT / "src" / "config")
_ensure_package("src.services", ROOT / "src" / "services")
_load_module("src.config.constants", ROOT / "src" / "config" / "constants.py")
_load_module("src.services.base", ROOT / "src" / "services" / "base.py")
luckmail_module = _load_module("src.services.luckmail_mail", ROOT / "src" / "services" / "luckmail_mail.py")
rust_cli_module = _load_module("src.services.luckmail_rust_cli", ROOT / "src" / "services" / "luckmail_rust_cli.py")
LuckMailService = luckmail_module.LuckMailService


class FakeUser:
    def __init__(self):
        self.calls = []
        self.purchase_pages = []
        self.purchase_results = []
        self.create_order_results = []
        self.alive_results = {}
        self.token_code_results = {}
        self.token_mail_results = {}
        self.token_mail_detail_results = {}

    def _take(self, mapping, key):
        value = mapping.get(key)
        if isinstance(value, list):
            if not value:
                raise AssertionError(f"No prepared result for {key}")
            if len(value) > 1:
                return value.pop(0)
            return value[0]
        return value

    def get_purchases(self, page=1, page_size=100, user_disabled=0):
        self.calls.append(("get_purchases", page, page_size, user_disabled))
        items = []
        if 0 <= page - 1 < len(self.purchase_pages):
            items = self.purchase_pages[page - 1]
        return SimpleNamespace(list=items)

    def purchase_emails(self, **kwargs):
        self.calls.append(("purchase_emails", kwargs))
        if not self.purchase_results:
            raise AssertionError("No prepared purchase result")
        result = self.purchase_results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    def create_order(self, **kwargs):
        self.calls.append(("create_order", kwargs))
        if not self.create_order_results:
            raise AssertionError("No prepared create-order result")
        result = self.create_order_results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    def check_token_alive(self, token):
        self.calls.append(("check_token_alive", token))
        result = self._take(self.alive_results, token)
        if result is None:
            raise AssertionError(f"No prepared alive result for {token}")
        return result

    def get_token_code(self, token):
        self.calls.append(("get_token_code", token))
        result = self._take(self.token_code_results, token)
        if result is None:
            raise AssertionError(f"No prepared token-code result for {token}")
        if isinstance(result, Exception):
            raise result
        return result

    def get_token_mails(self, token):
        self.calls.append(("get_token_mails", token))
        result = self._take(self.token_mail_results, token)
        if result is None:
            raise AssertionError(f"No prepared token-mail result for {token}")
        if isinstance(result, dict):
            return result
        return SimpleNamespace(mails=result)

    def get_token_mail_detail(self, token, message_id):
        self.calls.append(("get_token_mail_detail", token, message_id))
        key = (token, message_id)
        result = self._take(self.token_mail_detail_results, key)
        if result is None:
            raise AssertionError(f"No prepared token-mail detail result for {key}")
        return result

    def get_balance(self):
        self.calls.append(("get_balance",))
        return "10.0000"


class FakeClient:
    shared_user = None

    def __init__(self, base_url, api_key):
        self.base_url = base_url
        self.api_key = api_key
        self.user = self.__class__.shared_user


def build_service(monkeypatch, user, config=None, python_client_cls=FakeClient, rust_cli_path=None):
    FakeClient.shared_user = user
    if python_client_cls is None:
        monkeypatch.setattr(luckmail_module, "_load_luckmail_client_class", lambda: (None, ""))
    else:
        monkeypatch.setattr(luckmail_module, "_load_luckmail_client_class", lambda: (python_client_cls, "python:test"))
    monkeypatch.setattr(luckmail_module, "_has_luckmail_rust_sdk_assets", lambda: True)
    monkeypatch.setattr(luckmail_module, "_BATCH_REUSE_POOLS", {})
    monkeypatch.setattr(luckmail_module, "_BATCH_REUSE_PREPARED", set())
    monkeypatch.setattr(
        luckmail_module,
        "resolve_luckmail_rust_cli_path",
        lambda current_config: Path(rust_cli_path) if rust_cli_path else None,
    )
    service = LuckMailService({"api_key": "lm-test", **(config or {})}, name="luckmail-test")
    service._registered_file = Path("memory_registered.json")
    service._failed_file = Path("memory_failed.json")

    index_store = {
        "registered": {},
        "failed": {},
    }

    def fake_load(path):
        key = "registered" if path == service._registered_file else "failed"
        return {email: dict(meta) for email, meta in index_store[key].items()}

    def fake_save(path, index):
        key = "registered" if path == service._registered_file else "failed"
        index_store[key] = {email: dict(meta) for email, meta in index.items()}

    service._load_email_index = fake_load
    service._save_email_index = fake_save
    service._index_store = index_store
    return service


def test_create_email_skips_reused_purchase_that_fails_alive(monkeypatch):
    user = FakeUser()
    user.purchase_pages = [[
        {"id": 101, "email_address": "dead@example.com", "token": "tok-dead"},
        {"id": 102, "email_address": "live@example.com", "token": "tok-live"},
    ]]
    user.alive_results = {
        "tok-dead": [{"alive": False, "status": "failed", "message": "mailbox unavailable"}],
        "tok-live": [{"alive": True, "status": "ok", "message": "ready", "mail_count": 0}],
    }

    service = build_service(monkeypatch, user)

    result = service.create_email()

    assert result["email"] == "live@example.com"
    failed = service._index_store["failed"]
    assert "dead@example.com" in failed
    assert failed["dead@example.com"]["reason"].startswith("LuckMail 邮箱可用性检查失败")


def test_create_email_retries_new_purchase_until_token_is_alive(monkeypatch):
    user = FakeUser()
    user.purchase_results = [
        {"purchases": [{"id": 201, "email_address": "retry1@example.com", "token": "tok-retry-1"}]},
        {"purchases": [{"id": 202, "email_address": "ready@example.com", "token": "tok-ready"}]},
    ]
    user.alive_results = {
        "tok-retry-1": [{"alive": False, "status": "failed", "message": "expired"}],
        "tok-ready": [{"alive": True, "status": "ok", "message": "ready", "mail_count": 1}],
    }

    service = build_service(
        monkeypatch,
        user,
        config={
            "reuse_existing_purchases": False,
            "purchase_ready_retries": 2,
        },
    )

    result = service.create_email()

    assert result["email"] == "ready@example.com"
    purchase_calls = [call for call in user.calls if call[0] == "purchase_emails"]
    assert len(purchase_calls) == 2
    failed = service._index_store["failed"]
    assert "retry1@example.com" in failed


def test_create_email_falls_back_to_order_when_purchase_has_no_stock(monkeypatch):
    user = FakeUser()
    user.purchase_results = [
        RuntimeError('LuckMail Rust CLI 执行失败: Error: Api { code: 2003, message: "无库存", data: None }')
    ]
    user.create_order_results = [
        {"order_no": "ord-fallback-1", "email_address": "fallback-order@example.com"}
    ]

    service = build_service(
        monkeypatch,
        user,
        config={
            "reuse_existing_purchases": False,
        },
    )

    result = service.create_email()

    assert result["email"] == "fallback-order@example.com"
    assert result["inbox_mode"] == "order"
    assert result["order_no"] == "ord-fallback-1"
    assert result["source"] == "purchase_no_stock_fallback_order"
    assert ("purchase_emails", {"project_code": "openai", "quantity": 1, "email_type": "ms_graph"}) in user.calls
    assert ("create_order", {"project_code": "openai", "email_type": "ms_graph"}) in user.calls


def test_get_verification_code_falls_back_to_mail_list_and_detail(monkeypatch):
    user = FakeUser()
    user.token_code_results = {
        "tok-mail": [{"has_new_mail": False, "verification_code": ""}],
    }
    user.token_mail_results = {
        "tok-mail": [[
            {
                "message_id": "msg-1",
                "received_at": "2026-04-05T12:00:05Z",
                "subject": "Your verification code",
            }
        ]],
    }
    user.token_mail_detail_results = {
        ("tok-mail", "msg-1"): [
            {
                "message_id": "msg-1",
                "body_text": "Your OpenAI verification code is 654321",
                "verification_code": "654321",
            }
        ],
    }

    service = build_service(monkeypatch, user)
    service._cache_order(
        {
            "service_id": "tok-mail",
            "order_no": "",
            "email": "otp@example.com",
            "token": "tok-mail",
            "purchase_id": "301",
            "inbox_mode": "purchase",
            "project_code": "openai",
            "email_type": "ms_graph",
            "preferred_domain": "",
            "source": "new_purchase",
        }
    )

    code = service.get_verification_code(
        email="otp@example.com",
        email_id="tok-mail",
        timeout=1,
        otp_sent_at=1_775_390_400.0,
    )

    assert code == "654321"
    assert ("get_token_mails", "tok-mail") in user.calls
    assert ("get_token_mail_detail", "tok-mail", "msg-1") in user.calls


def test_get_verification_code_falls_back_to_mail_detail_when_token_code_errors(monkeypatch):
    user = FakeUser()
    user.token_code_results = {
        "tok-error": [RuntimeError("token code api unavailable")],
    }
    user.token_mail_results = {
        "tok-error": [[
            {
                "message_id": "msg-err-1",
                "received_at": "2026-04-05T12:00:05Z",
                "subject": "Your verification code",
            }
        ]],
    }
    user.token_mail_detail_results = {
        ("tok-error", "msg-err-1"): [
            {
                "message_id": "msg-err-1",
                "body_text": "Your OpenAI verification code is 246810",
                "verification_code": "246810",
            }
        ],
    }

    service = build_service(monkeypatch, user)
    service._cache_order(
        {
            "service_id": "tok-error",
            "order_no": "",
            "email": "otp-error@example.com",
            "token": "tok-error",
            "purchase_id": "302",
            "inbox_mode": "purchase",
            "project_code": "openai",
            "email_type": "ms_graph",
            "preferred_domain": "",
            "source": "new_purchase",
        }
    )

    code = service.get_verification_code(
        email="otp-error@example.com",
        email_id="tok-error",
        timeout=1,
        otp_sent_at=1_775_390_400.0,
    )

    assert code == "246810"
    assert ("get_token_code", "tok-error") in user.calls
    assert ("get_token_mails", "tok-error") in user.calls
    assert ("get_token_mail_detail", "tok-error", "msg-err-1") in user.calls

def test_resolve_luckmail_rust_cli_path_prefers_installed_binary(monkeypatch, tmp_path):
    repo_root = tmp_path / "repo"
    repo_binary_dir = repo_root / "LuckMailSdk-Rust" / "target" / "release"
    repo_binary_dir.mkdir(parents=True)

    binary_name = "luckmail-cli.exe" if sys.platform.startswith("win") else "luckmail-cli"
    installed_binary = tmp_path / binary_name
    repo_binary = repo_binary_dir / binary_name
    installed_binary.write_text("installed", encoding="utf-8")
    repo_binary.write_text("repo", encoding="utf-8")

    monkeypatch.setattr(rust_cli_module, "_repo_root", lambda: repo_root)
    monkeypatch.setattr(
        rust_cli_module.shutil,
        "which",
        lambda current_name: str(installed_binary) if current_name == binary_name else None,
    )

    assert rust_cli_module.resolve_luckmail_rust_cli_path({}) == installed_binary.resolve()



def test_check_health_prefers_rust_cli_backend_when_available(monkeypatch):
    user = FakeUser()
    rust_calls = []

    def fake_run_json(self, *args, timeout_seconds=None):
        rust_calls.append(args)
        if args and args[0] == "get-balance":
            return {"balance": "88.0000"}
        raise AssertionError(f"Unexpected Rust CLI call: {args}")

    monkeypatch.setattr(rust_cli_module.LuckMailRustCliBackend, "_run_json", fake_run_json)
    service = build_service(
        monkeypatch,
        user,
        config={"sdk_preference": "rust"},
        rust_cli_path="d:/codex-console-test/fake-luckmail-cli.exe",
    )

    assert service.get_service_info()["sdk_backend"] == "rust:cli"
    assert service.check_health() is True
    assert rust_calls == [("get-balance",)]
    assert user.calls == []


def test_check_health_falls_back_to_python_when_rust_cli_fails(monkeypatch):
    user = FakeUser()

    def fake_run_json(self, *args, timeout_seconds=None):
        raise rust_cli_module.LuckMailRustCliError("rust cli unavailable")

    monkeypatch.setattr(rust_cli_module.LuckMailRustCliBackend, "_run_json", fake_run_json)
    service = build_service(
        monkeypatch,
        user,
        config={"sdk_preference": "rust"},
        rust_cli_path="d:/codex-console-test/fake-luckmail-cli.exe",
    )

    assert service.check_health() is True
    assert ("get_balance",) in user.calls


def test_service_supports_rust_cli_backend_without_python_sdk(monkeypatch):
    user = FakeUser()
    rust_calls = []

    def fake_run_json(self, *args, timeout_seconds=None):
        rust_calls.append(args)
        if args and args[0] == "get-balance":
            return {"balance": "18.5000"}
        raise AssertionError(f"Unexpected Rust CLI call: {args}")

    monkeypatch.setattr(rust_cli_module.LuckMailRustCliBackend, "_run_json", fake_run_json)
    service = build_service(
        monkeypatch,
        user,
        config={"sdk_preference": "rust"},
        python_client_cls=None,
        rust_cli_path="d:/codex-console-test/fake-luckmail-cli.exe",
    )

    assert service.client is None
    assert service.get_service_info()["sdk_backend"] == "rust:cli"
    assert service.check_health() is True
    assert rust_calls == [("get-balance",)]


def test_backend_check_token_alive_passes_request_timeout_to_rust_cli(monkeypatch):
    user = FakeUser()
    rust_calls = []

    def fake_check_token_alive(self, token, timeout_seconds=None):
        rust_calls.append((token, timeout_seconds))
        return {"alive": True, "status": "ok", "mail_count": 0}

    monkeypatch.setattr(rust_cli_module.LuckMailRustCliBackend, "check_token_alive", fake_check_token_alive)
    service = build_service(
        monkeypatch,
        user,
        config={"sdk_preference": "rust"},
        rust_cli_path="d:/codex-console-test/fake-luckmail-cli.exe",
    )

    result = service._backend_check_token_alive("tok-fast", request_timeout_seconds=4)

    assert result["alive"] is True
    assert rust_calls == [("tok-fast", 4)]
    assert user.calls == []


def test_pick_reusable_purchase_inbox_limits_alive_candidates(monkeypatch):
    user = FakeUser()
    user.purchase_pages = [[
        {"id": 101, "email_address": "first@example.com", "token": "tok-first"},
        {"id": 102, "email_address": "second@example.com", "token": "tok-second"},
        {"id": 103, "email_address": "third@example.com", "token": "tok-third"},
    ]]
    user.alive_results = {
        "tok-first": [{"alive": False, "status": "failed", "message": "invalid"}],
        "tok-second": [{"alive": False, "status": "failed", "message": "invalid"}],
        "tok-third": [{"alive": True, "status": "ok", "message": "ready", "mail_count": 0}],
    }

    service = build_service(
        monkeypatch,
        user,
        config={"reuse_purchase_candidate_limit": 2},
    )

    result = service._pick_reusable_purchase_inbox(
        project_code="openai",
        email_type="ms_graph",
        preferred_domain="",
    )

    assert result is None
    alive_calls = [call for call in user.calls if call[0] == "check_token_alive"]
    assert [call[1] for call in alive_calls] == ["tok-first", "tok-second"]


def test_reuse_purchase_alive_check_uses_single_attempt(monkeypatch):
    user = FakeUser()
    user.alive_results = {
        "tok-reuse": [
            {"alive": False, "status": "pending", "message": "warming"},
            {"alive": True, "status": "ok", "message": "ready", "mail_count": 0},
        ]
    }

    service = build_service(monkeypatch, user)

    ready = service._ensure_purchase_inbox_ready(
        {
            "email": "reuse@example.com",
            "token": "tok-reuse",
            "inbox_mode": "purchase",
            "source": "reuse_purchase",
        }
    )

    assert ready is False
    alive_calls = [call for call in user.calls if call[0] == "check_token_alive"]
    assert alive_calls == [("check_token_alive", "tok-reuse")]


def test_pick_reusable_purchase_inbox_logs_when_no_candidates(monkeypatch, caplog):
    user = FakeUser()
    user.purchase_pages = [[]]

    service = build_service(monkeypatch, user)

    with caplog.at_level(logging.INFO, logger=luckmail_module.logger.name):
        result = service._pick_reusable_purchase_inbox(
            project_code="openai",
            email_type="ms_imap",
            preferred_domain="hotmail.com",
        )

    assert result is None
    assert any(
        "未找到可复用已购邮箱候选" in record.message
        for record in caplog.records
    )
    alive_calls = [call for call in user.calls if call[0] == "check_token_alive"]
    assert alive_calls == []



def test_prepare_batch_reusable_inboxes_prefills_pool_and_create_email_consumes_it(monkeypatch):
    user = FakeUser()
    user.purchase_pages = [[
        {"id": 101, "email_address": "first@example.com", "token": "tok-first"},
        {"id": 102, "email_address": "second@example.com", "token": "tok-second"},
    ]]
    user.alive_results = {
        "tok-first": [{"alive": True, "status": "ok", "message": "ready", "mail_count": 0}],
        "tok-second": [{"alive": True, "status": "ok", "message": "ready", "mail_count": 1}],
    }

    service = build_service(monkeypatch, user)

    prepared_count = service.prepare_batch_reusable_inboxes("batch-prefill", 2)
    first = service.create_email({"batch_id": "batch-prefill"})
    second = service.create_email({"batch_id": "batch-prefill"})

    assert prepared_count == 2
    assert first["email"] == "first@example.com"
    assert second["email"] == "second@example.com"
    get_purchase_calls = [call for call in user.calls if call[0] == "get_purchases"]
    assert get_purchase_calls == [("get_purchases", 1, 100, 0)]
    alive_calls = [call for call in user.calls if call[0] == "check_token_alive"]
    assert [call[1] for call in alive_calls] == ["tok-first", "tok-second"]


def test_create_email_skips_rescan_when_batch_reuse_pool_is_prepared_but_empty(monkeypatch):
    user = FakeUser()
    user.purchase_results = [
        {"purchases": [{"id": 301, "email_address": "new@example.com", "token": "tok-new"}]}
    ]
    user.alive_results = {
        "tok-new": [{"alive": True, "status": "ok", "message": "ready", "mail_count": 0}],
    }

    service = build_service(monkeypatch, user)
    service.prepare_batch_reusable_inboxes("batch-empty", 1)

    def fail_if_rescan(*args, **kwargs):
        raise AssertionError("batch prepared flow should not rescan reusable purchases")

    monkeypatch.setattr(service, "_pick_reusable_purchase_inbox", fail_if_rescan)

    result = service.create_email({"batch_id": "batch-empty"})

    assert result["email"] == "new@example.com"
    purchase_calls = [call for call in user.calls if call[0] == "purchase_emails"]
    assert len(purchase_calls) == 1


def test_run_batch_purchase_action_waits_for_next_slot(monkeypatch):
    user = FakeUser()
    service = build_service(monkeypatch, user)

    luckmail_module.LuckMailService.clear_batch_reuse_pool("batch-gate")
    luckmail_module._BATCH_PURCHASE_NEXT_ALLOWED_AT["batch-gate"] = 101.25
    sleep_calls = []
    now = {"value": 100.0}

    def fake_time():
        return now["value"]

    def fake_sleep(seconds):
        sleep_calls.append(seconds)
        now["value"] += seconds

    monkeypatch.setattr(luckmail_module.time, "time", fake_time)
    monkeypatch.setattr(luckmail_module.time, "sleep", fake_sleep)

    result = service._run_batch_purchase_action("batch-gate", "purchase_emails", lambda: "ok")

    assert result == "ok"
    assert sleep_calls == [1.25]
    assert luckmail_module._BATCH_PURCHASE_NEXT_ALLOWED_AT["batch-gate"] == 102.25
    luckmail_module.LuckMailService.clear_batch_reuse_pool("batch-gate")


def test_create_email_routes_batch_new_purchase_through_gate(monkeypatch):
    user = FakeUser()
    user.purchase_results = [
        {"purchases": [{"id": 401, "email_address": "gate@example.com", "token": "tok-gate"}]}
    ]
    user.alive_results = {
        "tok-gate": [{"alive": True, "status": "ok", "message": "ready", "mail_count": 0}],
    }

    service = build_service(
        monkeypatch,
        user,
        config={"reuse_existing_purchases": False},
    )
    gate_calls = []

    def fake_batch_gate(batch_id, action, func):
        gate_calls.append((batch_id, action))
        return func()

    monkeypatch.setattr(service, "_run_batch_purchase_action", fake_batch_gate)

    result = service.create_email({"batch_id": "batch-purchase"})

    assert result["email"] == "gate@example.com"
    assert gate_calls == [("batch-purchase", "purchase_emails")]


def test_create_order_uses_batch_gate_when_batch_id_present(monkeypatch):
    user = FakeUser()
    user.create_order_results = [
        {"order_no": "ord-batch-1", "email_address": "order-batch@example.com"}
    ]

    service = build_service(monkeypatch, user)
    gate_calls = []

    def fake_batch_gate(batch_id, action, func):
        gate_calls.append((batch_id, action))
        return func()

    monkeypatch.setattr(service, "_run_batch_purchase_action", fake_batch_gate)

    result = service._create_order_inbox(
        project_code="openai",
        email_type="ms_graph",
        preferred_domain="hotmail.com",
        batch_id="batch-order",
    )

    assert result["order_no"] == "ord-batch-1"
    assert gate_calls == [("batch-order", "create_order")]
