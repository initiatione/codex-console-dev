from types import SimpleNamespace

from src.core.upload import codex2api_upload


class FakeResponse:
    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self):
        if self._payload is None:
            raise ValueError("no json payload")
        return self._payload


class FakeMime:
    def __init__(self):
        self.parts = []

    def addpart(self, **kwargs):
        self.parts.append(kwargs)


def make_account(**kwargs):
    base = {
        "id": 1,
        "email": "tester@example.com",
        "access_token": "at",
        "refresh_token": "rt",
    }
    base.update(kwargs)
    return SimpleNamespace(**base)


def test_build_codex2api_import_entries_skips_empty_tokens():
    accounts = [
        make_account(id=1, email="a@example.com", access_token="at-1", refresh_token=""),
        make_account(id=2, email="b@example.com", access_token="", refresh_token="rt-2"),
        make_account(id=3, email="c@example.com", access_token="", refresh_token=""),
    ]

    entries = codex2api_upload.build_codex2api_import_entries(accounts)

    assert entries == [
        {"refresh_token": "", "access_token": "at-1", "email": "a@example.com"},
        {"refresh_token": "rt-2", "access_token": "", "email": "b@example.com"},
    ]


def test_upload_to_codex2api_posts_import_request(monkeypatch):
    calls = {}

    def fake_mime():
        calls["mime"] = FakeMime()
        return calls["mime"]

    def fake_post(url, **kwargs):
        calls["url"] = url
        calls["kwargs"] = kwargs
        return FakeResponse(status_code=200, payload={"message": "导入成功"})

    monkeypatch.setattr(codex2api_upload, "CurlMime", fake_mime)
    monkeypatch.setattr(codex2api_upload.cffi_requests, "post", fake_post)

    success, message = codex2api_upload.upload_to_codex2api(
        [make_account()],
        "https://codex2api.example.com/",
        "admin-key",
    )

    assert success is True
    assert message == "导入成功"
    assert calls["url"] == "https://codex2api.example.com/api/admin/accounts/import"
    assert calls["kwargs"]["headers"]["X-Admin-Key"] == "admin-key"
    assert calls["kwargs"]["multipart"] is calls["mime"]
    assert len(calls["mime"].parts) == 2
    assert calls["mime"].parts[1]["name"] == "format"
    assert calls["mime"].parts[1]["data"] == b"json"


def test_test_codex2api_connection_uses_health_endpoint(monkeypatch):
    calls = {}

    def fake_get(url, **kwargs):
        calls["url"] = url
        calls["kwargs"] = kwargs
        return FakeResponse(status_code=200, payload={"status": "ok"})

    monkeypatch.setattr(codex2api_upload.cffi_requests, "get", fake_get)

    success, message = codex2api_upload.test_codex2api_connection(
        "https://codex2api.example.com",
        "admin-key",
    )

    assert success is True
    assert message == "Codex2API 连接测试成功"
    assert calls["url"] == "https://codex2api.example.com/api/admin/health"
    assert calls["kwargs"]["headers"]["X-Admin-Key"] == "admin-key"
