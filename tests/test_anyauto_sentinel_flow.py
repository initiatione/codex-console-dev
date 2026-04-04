import json

import src.core.anyauto.chatgpt_client as chatgpt_module
import src.core.anyauto.sentinel_token as sentinel_token_module
from src.core.anyauto.chatgpt_client import ChatGPTClient


class DummyResponse:
    def __init__(self, status_code=200, payload=None, text="", url="https://auth.openai.com/about-you"):
        self.status_code = status_code
        self._payload = payload or {}
        self.text = text
        self.url = url

    def json(self):
        return self._payload


class DummySession:
    def __init__(self):
        self.calls = []

    def post(self, url, **kwargs):
        self.calls.append({"url": url, "kwargs": kwargs})
        return DummyResponse(status_code=200, payload={"page": {"type": "about_you"}}, url=url)


def _header_value(headers, name: str):
    for key, value in (headers or {}).items():
        if str(key).lower() == name.lower():
            return value
    return None


def test_register_user_prefers_browser_sentinel_token(monkeypatch):
    captured = {}

    def fake_browser_token(**kwargs):
        captured["flow"] = kwargs.get("flow")
        captured["page_url"] = kwargs.get("page_url")
        return '{"source":"browser"}'

    monkeypatch.setattr(chatgpt_module, "get_sentinel_token_via_browser", fake_browser_token)
    monkeypatch.setattr(chatgpt_module, "build_sentinel_token", lambda *args, **kwargs: '{"source":"pow"}')

    client = ChatGPTClient(verbose=False, browser_mode="protocol")
    client.session = DummySession()
    client.device_id = "device-fixed"

    ok, _ = client.register_user("tester@example.com", "Aa1!passWORD")

    assert ok is True
    assert captured["flow"] == "username_password_create"
    assert captured["page_url"] == "https://auth.openai.com/create-account/password"
    headers = client.session.calls[0]["kwargs"]["headers"]
    assert _header_value(headers, "oai-device-id") == "device-fixed"
    assert _header_value(headers, "openai-sentinel-token") == '{"source":"browser"}'


def test_create_account_uses_oauth_create_account_flow(monkeypatch):
    captured = {}

    def fake_browser_token(**kwargs):
        captured["flow"] = kwargs.get("flow")
        captured["page_url"] = kwargs.get("page_url")
        return '{"source":"browser"}'

    monkeypatch.setattr(chatgpt_module, "get_sentinel_token_via_browser", fake_browser_token)
    monkeypatch.setattr(chatgpt_module, "build_sentinel_token", lambda *args, **kwargs: '{"source":"pow"}')

    client = ChatGPTClient(verbose=False, browser_mode="protocol")
    client.session = DummySession()
    client.device_id = "device-fixed"

    ok, _ = client.create_account("Test", "User", "1998-01-01")

    assert ok is True
    assert captured["flow"] == "oauth_create_account"
    assert captured["page_url"] == "https://auth.openai.com/about-you"
    headers = client.session.calls[0]["kwargs"]["headers"]
    assert _header_value(headers, "oai-device-id") == "device-fixed"
    assert _header_value(headers, "openai-sentinel-token") == '{"source":"browser"}'


def test_get_sentinel_token_falls_back_to_http_pow(monkeypatch):
    called = {"browser": 0, "pow": 0}

    def fake_browser_token(**kwargs):
        called["browser"] += 1
        return None

    def fake_pow_token(*args, **kwargs):
        called["pow"] += 1
        assert kwargs.get("flow") == "oauth_create_account"
        return '{"source":"pow"}'

    monkeypatch.setattr(chatgpt_module, "get_sentinel_token_via_browser", fake_browser_token)
    monkeypatch.setattr(chatgpt_module, "build_sentinel_token", fake_pow_token)

    client = ChatGPTClient(verbose=False, browser_mode="protocol")
    token = client._get_sentinel_token("oauth_create_account", page_url="https://auth.openai.com/about-you")

    assert token == '{"source":"pow"}'
    assert called["browser"] == 1
    assert called["pow"] == 1


def test_build_sentinel_token_uses_turnstile_value(monkeypatch):
    monkeypatch.setattr(
        sentinel_token_module,
        "fetch_sentinel_challenge",
        lambda *args, **kwargs: {
            "token": "challenge-c",
            "turnstile": {"token": "turnstile-token"},
            "proofofwork": {"required": False},
        },
    )
    monkeypatch.setattr(
        sentinel_token_module.SentinelTokenGenerator,
        "generate_requirements_token",
        lambda self: "gAAAAACreq",
    )

    token = sentinel_token_module.build_sentinel_token(
        session=object(),
        device_id="device-1",
        flow="oauth_create_account",
    )
    payload = json.loads(token)

    assert payload["flow"] == "oauth_create_account"
    assert payload["id"] == "device-1"
    assert payload["p"] == "gAAAAACreq"
    assert payload["c"] == "challenge-c"
    assert payload["t"] == "turnstile-token"
