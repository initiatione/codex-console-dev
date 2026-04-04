import json

from src.config.constants import EmailServiceType, OPENAI_API_ENDPOINTS, OPENAI_PAGE_TYPES
from src.core.register import RegistrationEngine
from src.services.base import BaseEmailService


class DummyEmailService(BaseEmailService):
    def __init__(self):
        super().__init__(EmailServiceType.TEMPMAIL)

    def create_email(self, config=None):
        return {"email": "tester@example.com", "service_id": "mailbox-1"}

    def get_verification_code(self, *args, **kwargs):
        return "123456"

    def list_emails(self, **kwargs):
        return []

    def delete_email(self, email_id):
        return True

    def check_health(self):
        return True


class DummyResponse:
    def __init__(self, status_code=200, payload=None, text="", headers=None):
        self.status_code = status_code
        self._payload = payload
        self.text = text
        self.headers = headers or {}

    def json(self):
        if self._payload is None:
            raise ValueError("no json payload")
        return self._payload


class CaptureSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []
        self.cookies = {"oai-did": "device-fixed"}

    def post(self, url, **kwargs):
        self.calls.append({"method": "POST", "url": url, "kwargs": kwargs})
        if not self.responses:
            raise AssertionError(f"unexpected POST {url}")
        return self.responses.pop(0)

    def get(self, url, **kwargs):
        self.calls.append({"method": "GET", "url": url, "kwargs": kwargs})
        if not self.responses:
            raise AssertionError(f"unexpected GET {url}")
        return self.responses.pop(0)


def _header_value(headers, name: str):
    for key, value in (headers or {}).items():
        if str(key).lower() == name.lower():
            return value
    return None


def _make_engine(session):
    engine = RegistrationEngine(DummyEmailService())
    engine.session = session
    engine.email = "tester@example.com"
    engine.password = "Aa1!passWORD"
    engine.device_id = "device-fixed"
    engine._log = lambda *args, **kwargs: None
    return engine


def test_submit_auth_start_uses_full_sentinel_token_without_wrapping():
    session = CaptureSession([
        DummyResponse(payload={"page": {"type": OPENAI_PAGE_TYPES["PASSWORD_REGISTRATION"]}}),
    ])
    engine = _make_engine(session)

    result = engine._submit_auth_start(
        "device-fixed",
        '{"source":"full-token"}',
        screen_hint="signup",
        referer="https://auth.openai.com/create-account",
        log_label="测试入口",
    )

    assert result.success is True
    headers = session.calls[0]["kwargs"]["headers"]
    assert _header_value(headers, "oai-device-id") == "device-fixed"
    assert _header_value(headers, "openai-sentinel-token") == '{"source":"full-token"}'


def test_submit_login_password_uses_password_verify_sentinel():
    session = CaptureSession([
        DummyResponse(payload={"page": {"type": OPENAI_PAGE_TYPES["EMAIL_OTP_VERIFICATION"]}}),
    ])
    engine = _make_engine(session)
    captured = []

    def fake_check_sentinel(did, *, flow="authorize_continue"):
        captured.append((did, flow))
        return '{"flow":"password_verify"}'

    engine._check_sentinel = fake_check_sentinel

    result = engine._submit_login_password()

    assert result.success is True
    assert captured == [("device-fixed", "password_verify")]
    headers = session.calls[0]["kwargs"]["headers"]
    assert _header_value(headers, "oai-device-id") == "device-fixed"
    assert _header_value(headers, "openai-sentinel-token") == '{"flow":"password_verify"}'


def test_register_password_uses_username_password_create_sentinel():
    session = CaptureSession([DummyResponse(payload={})])
    engine = _make_engine(session)
    captured = []

    def fake_check_sentinel(did, *, flow="authorize_continue"):
        captured.append((did, flow))
        return '{"flow":"username_password_create"}'

    engine._check_sentinel = fake_check_sentinel

    success, password = engine._register_password("device-fixed", None)

    assert success is True
    assert password
    assert captured == [("device-fixed", "username_password_create")]
    headers = session.calls[0]["kwargs"]["headers"]
    assert _header_value(headers, "oai-device-id") == "device-fixed"
    assert _header_value(headers, "openai-sentinel-token") == '{"flow":"username_password_create"}'


def test_validate_otp_uses_email_otp_validate_sentinel():
    session = CaptureSession([
        DummyResponse(payload={"continue_url": "/sign-in-with-chatgpt/codex/consent"}),
    ])
    engine = _make_engine(session)
    captured = []

    def fake_check_sentinel(did, *, flow="authorize_continue"):
        captured.append((did, flow))
        return '{"flow":"email_otp_validate"}'

    engine._check_sentinel = fake_check_sentinel

    success = engine._validate_verification_code("123456")

    assert success is True
    assert captured == [("device-fixed", "email_otp_validate")]
    headers = session.calls[0]["kwargs"]["headers"]
    assert _header_value(headers, "oai-device-id") == "device-fixed"
    assert _header_value(headers, "openai-sentinel-token") == '{"flow":"email_otp_validate"}'


def test_create_user_account_uses_oauth_create_account_sentinel():
    session = CaptureSession([
        DummyResponse(payload={"continue_url": "https://auth.openai.com/sign-in-with-chatgpt/codex/consent"}),
    ])
    engine = _make_engine(session)
    captured = []

    def fake_check_sentinel(did, *, flow="authorize_continue"):
        captured.append((did, flow))
        return '{"flow":"oauth_create_account"}'

    engine._check_sentinel = fake_check_sentinel

    success = engine._create_user_account()

    assert success is True
    assert captured == [("device-fixed", "oauth_create_account")]
    headers = session.calls[0]["kwargs"]["headers"]
    assert _header_value(headers, "oai-device-id") == "device-fixed"
    assert _header_value(headers, "openai-sentinel-token") == '{"flow":"oauth_create_account"}'


def test_select_workspace_handles_organization_select_redirect():
    session = CaptureSession([
        DummyResponse(
            payload={
                "continue_url": "/sign-in-with-chatgpt/codex/organization",
                "data": {
                    "orgs": [
                        {
                            "id": "org-123",
                            "projects": [{"id": "proj-123"}],
                        }
                    ]
                },
            }
        ),
        DummyResponse(
            status_code=302,
            headers={"Location": "http://localhost:1455/auth/callback?code=auth-code&state=oauth-state"},
        ),
    ])
    engine = _make_engine(session)

    continue_url = engine._select_workspace("ws-123")

    assert continue_url == "http://localhost:1455/auth/callback?code=auth-code&state=oauth-state"
    assert len(session.calls) == 2
    workspace_headers = session.calls[0]["kwargs"]["headers"]
    org_headers = session.calls[1]["kwargs"]["headers"]
    assert _header_value(workspace_headers, "oai-device-id") == "device-fixed"
    assert _header_value(org_headers, "oai-device-id") == "device-fixed"
    assert json.loads(session.calls[1]["kwargs"]["data"]) == {"org_id": "org-123", "project_id": "proj-123"}
