from src.services.duckduckgo_cloudmail import DuckDuckGoCloudMailService


class FakeResponse:
    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload
        self.text = text
        self.headers = {}

    def json(self):
        if self._payload is None:
            raise ValueError("no json payload")
        return self._payload


class FakeHTTPClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append({
            "method": method,
            "url": url,
            "kwargs": kwargs,
        })
        if not self.responses:
            raise AssertionError(f"未准备响应: {method} {url}")
        return self.responses.pop(0)


def make_service():
    return DuckDuckGoCloudMailService({
        "bridge_base_url": "http://127.0.0.1:1456",
        "bridge_token": "bridge-secret",
        "forward_to_email": "relay@example.com",
        "cloudmail_base_url": "https://cloudmail.test",
        "cloudmail_admin_email": "admin@cloudmail.test",
        "cloudmail_admin_password": "cm-secret",
        "timeout": 5,
        "poll_interval": 1,
    })


def test_create_email_leases_alias_from_bridge():
    service = make_service()
    fake_client = FakeHTTPClient([
        FakeResponse(
            payload={
                "lease_id": "lease-1",
                "alias_email": "alias@duck.com",
                "forward_to_email": "relay@example.com",
                "alias_state": "leased",
                "generated_at": "2026-04-06T06:00:00Z",
            }
        )
    ])
    service.http_client = fake_client

    lease = service.create_email()

    assert lease["service_id"] == "lease-1"
    assert lease["email"] == "alias@duck.com"
    assert lease["forward_to_email"] == "relay@example.com"

    call = fake_client.calls[0]
    assert call["method"] == "POST"
    assert call["url"] == "http://127.0.0.1:1456/api/v1/aliases/lease"
    assert call["kwargs"]["headers"]["Authorization"] == "Bearer bridge-secret"
    assert call["kwargs"]["json"]["consumer"] == "codex-console-back"


def test_get_verification_code_matches_alias_specific_cloudmail_message():
    service = make_service()
    service._cache_lease({
        "lease_id": "lease-1",
        "service_id": "lease-1",
        "alias_email": "alias@duck.com",
        "forward_to_email": "relay@example.com",
    })
    service._fetch_cloudmail_messages = lambda: [
        {
            "emailId": "msg-1",
            "createTime": "2026-04-06T06:00:05Z",
            "sendEmail": "noreply@openai.com",
            "subject": "OpenAI verification code",
            "recipient": "alias@duck.com",
            "toEmail": "relay@example.com",
            "messageId": "<alias@duck.com>",
            "content": "Your OpenAI verification code is 654321",
        }
    ]

    code = service.get_verification_code(
        email="alias@duck.com",
        email_id="lease-1",
        timeout=1,
        otp_sent_at=0,
    )

    assert code == "654321"


def test_mark_registration_outcome_posts_bridge_update_and_clears_cache():
    service = make_service()
    service._cache_lease({
        "lease_id": "lease-1",
        "service_id": "lease-1",
        "alias_email": "alias@duck.com",
        "forward_to_email": "relay@example.com",
    })
    fake_client = FakeHTTPClient([FakeResponse(payload={"ok": True})])
    service.http_client = fake_client

    service.mark_registration_outcome(
        email="alias@duck.com",
        success=True,
        reason="completed",
        context={"service_id": "lease-1", "task_uuid": "task-1"},
    )

    call = fake_client.calls[0]
    assert call["method"] == "POST"
    assert call["url"] == "http://127.0.0.1:1456/api/v1/aliases/lease-1/mark-outcome"
    assert call["kwargs"]["json"]["success"] is True
    assert call["kwargs"]["json"]["reason"] == "completed"
    assert service._leases_by_id == {}
    assert service._leases_by_email == {}
