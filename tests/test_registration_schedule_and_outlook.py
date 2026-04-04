import asyncio
from contextlib import contextmanager

from src.web.routes import registration


class DummyQuery:
    def __init__(self, mapping, first_result=None):
        self.mapping = mapping
        self.first_result = first_result

    def filter(self, *args, **kwargs):
        return self

    def first(self):
        return self.first_result


class DummyDB:
    def __init__(self, services=None, accounts=None):
        self.services = services or {}
        self.accounts = accounts or {}

    def query(self, model):
        name = getattr(model, "__name__", "")
        if name == "EmailService":
            query = DummyQuery(self.services)
            query.first = lambda: next(iter(self.services.values()), None)
            return query
        if name == "Account":
            query = DummyQuery(self.accounts)
            query.first = lambda: next(iter(self.accounts.values()), None)
            return query
        if name == "ScheduledRegistrationJob":
            return DummyQuery({})
        return DummyQuery({})


class DummyService:
    def __init__(self, service_id, email):
        self.id = service_id
        self.name = "Primary Outlook"
        self.config = {"email": email}


class DummyAccount:
    def __init__(self, email):
        self.email = email


class DummyScheduledJob:
    def __init__(self):
        self.job_uuid = "job-1"
        self.enabled = True
        self.is_running = False
        self.schedule_type = "interval"
        self.schedule_config = {"minutes": 30}
        self.registration_config = {"email_service_type": "tempmail", "reg_mode": "single"}
        self.run_count = 0


async def _fake_dispatch(config, background_tasks):
    return {"task_uuid": "task-123", "batch_id": None}


def test_outlook_batch_skip_registered_is_case_insensitive(monkeypatch):
    # 用假 DB 直接覆盖 skip_registered 逻辑，避免 Windows 下 sqlite 文件锁噪音。
    service = DummyService(service_id=7, email="Foo@Outlook.com")
    account = DummyAccount(email="foo@outlook.com")

    @contextmanager
    def fake_get_db():
        yield DummyDB(services={service.id: service}, accounts={1: account})

    captured_calls = []

    def fake_schedule(background_tasks, coroutine_func, *args):
        captured_calls.append((background_tasks, coroutine_func, args))

    monkeypatch.setattr(registration, "get_db", fake_get_db)
    monkeypatch.setattr(registration, "_schedule_async_job", fake_schedule)

    request = registration.OutlookBatchRegistrationRequest(
        service_ids=[service.id],
        skip_registered=True,
    )

    response = asyncio.run(registration._start_outlook_batch_registration_internal(request))

    assert response.batch_id == ""
    assert response.total == 1
    assert response.skipped == 1
    assert response.to_register == 0
    assert response.service_ids == []
    assert captured_calls == []


def test_run_scheduled_registration_job_now_marks_triggered_not_success(monkeypatch):
    # 只验证路由层状态流转语义：触发成功 != 执行成功。
    updates = []
    dummy_job = DummyScheduledJob()

    @contextmanager
    def fake_get_db():
        yield object()

    def fake_get_job(db, job_uuid):
        return dummy_job

    def fake_claim(db, job_uuid, next_run_at, now):
        return dummy_job

    def fake_update(db, job_uuid, **kwargs):
        updates.append((job_uuid, kwargs))
        return True

    monkeypatch.setattr(registration, "get_db", fake_get_db)
    monkeypatch.setattr(registration, "compute_next_run_at", lambda *args, **kwargs: None)
    monkeypatch.setattr(registration, "dispatch_registration_config", _fake_dispatch)
    monkeypatch.setattr(registration.crud, "get_scheduled_registration_job_by_uuid", fake_get_job)
    monkeypatch.setattr(registration.crud, "claim_scheduled_registration_job", fake_claim)
    monkeypatch.setattr(registration.crud, "update_scheduled_registration_job", fake_update)

    result = asyncio.run(registration.run_scheduled_registration_job_now("job-1", background_tasks=None))

    assert result["success"] is True
    assert result["task_uuid"] == "task-123"
    assert len(updates) == 1
    _, payload = updates[0]
    assert payload["status"] == "scheduled"
    assert payload["is_running"] is False
    assert payload["last_error"] is None
    assert payload["run_count"] == 1
    assert payload["consecutive_failures"] == 0
    assert payload["last_triggered_task_uuid"] == "task-123"
