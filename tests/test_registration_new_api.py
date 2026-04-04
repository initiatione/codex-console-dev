from datetime import datetime

from src.web.routes import registration


class DummyBackgroundTasks:
    def __init__(self):
        self.calls = []

    def add_task(self, func, *args):
        self.calls.append((func, args))


def test_start_single_registration_schedules_new_api_upload(monkeypatch):
    captured = {}

    def fake_validate(_):
        return None

    def fake_create_registration_task(db, task_uuid, proxy):
        return type(
            "Task",
            (),
            {
                "id": 1,
                "task_uuid": task_uuid,
                "status": "pending",
                "email_service_id": None,
                "proxy": proxy,
                "logs": None,
                "result": None,
                "error_message": None,
                "created_at": None,
                "started_at": None,
                "completed_at": None,
            },
        )()

    class DummyDb:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def fake_get_db():
        return DummyDb()

    def fake_schedule(background_tasks, coroutine_func, *args):
        captured["args"] = args

    monkeypatch.setattr(registration, "_validate_registration_request", fake_validate)
    monkeypatch.setattr(registration.crud, "create_registration_task", fake_create_registration_task)
    monkeypatch.setattr(registration, "get_db", fake_get_db)
    monkeypatch.setattr(registration, "_schedule_async_job", fake_schedule)

    request = registration.RegistrationTaskCreate(
        email_service_type="tempmail",
        auto_upload_new_api=True,
        new_api_service_ids=[1, 2],
        filter_only_access_token_accounts=False,
    )

    response = registration.asyncio.run(registration._start_single_registration_internal(request))

    assert response.status == "pending"
    assert captured["args"][-4] is True
    assert captured["args"][-3] == [1, 2]
    assert captured["args"][-2] is False
    assert captured["args"][-1] == "child"


def test_dispatch_registration_config_maps_new_api_fields_for_single(monkeypatch):
    captured = {}

    async def fake_single(request, background_tasks=None):
        captured["request"] = request
        return type("Response", (), {"task_uuid": "task-1", "model_dump": lambda self: {"task_uuid": "task-1"}})()

    monkeypatch.setattr(registration, "_start_single_registration_internal", fake_single)
    monkeypatch.setattr(registration, "_validate_registration_request", lambda _: None)

    result = registration.asyncio.run(
        registration.dispatch_registration_config(
            {
                "email_service_type": "tempmail",
                "auto_upload_new_api": True,
                "new_api_service_ids": [9],
                "filter_only_access_token_accounts": False,
            }
        )
    )

    assert result["kind"] == "single"
    assert captured["request"].auto_upload_new_api is True
    assert captured["request"].new_api_service_ids == [9]
    assert captured["request"].filter_only_access_token_accounts is False


def test_dispatch_registration_config_maps_new_api_fields_for_batch(monkeypatch):
    captured = {}

    async def fake_batch(request, background_tasks=None):
        captured["request"] = request
        return type("Response", (), {"batch_id": "batch-1", "model_dump": lambda self: {"batch_id": "batch-1"}})()

    monkeypatch.setattr(registration, "_start_batch_registration_internal", fake_batch)
    monkeypatch.setattr(registration, "_validate_registration_request", lambda _: None)

    result = registration.asyncio.run(
        registration.dispatch_registration_config(
            {
                "reg_mode": "batch",
                "email_service_type": "tempmail",
                "auto_upload_new_api": True,
                "new_api_service_ids": [3, 4],
                "filter_only_access_token_accounts": False,
            }
        )
    )

    assert result["kind"] == "batch"
    assert captured["request"].auto_upload_new_api is True
    assert captured["request"].new_api_service_ids == [3, 4]
    assert captured["request"].filter_only_access_token_accounts is False

def test_get_registration_stats_uses_finished_total_for_today_summary(monkeypatch):
    call_index = {"query": 0}

    class DummyQuery:
        def __init__(self, rows=None, scalar_value=None):
            self._rows = rows or []
            self._scalar_value = scalar_value

        def group_by(self, *_args):
            return self

        def filter(self, *_args):
            return self

        def all(self):
            return self._rows

        def scalar(self):
            return self._scalar_value

    class DummyDb:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def query(self, *_entities):
            call_index["query"] += 1
            if call_index["query"] == 1:
                return DummyQuery(rows=[("completed", 12), ("failed", 3), ("running", 2)])
            if call_index["query"] == 2:
                return DummyQuery(rows=[("completed", 39), ("failed", 14), ("running", 8)])
            return DummyQuery(scalar_value=61)

    monkeypatch.setattr(registration, "get_db", lambda: DummyDb())
    monkeypatch.setattr(registration, "utcnow_naive", lambda: datetime(2026, 4, 2, 12, 0, 0))

    result = registration.asyncio.run(registration.get_registration_stats())

    assert result["today_total"] == 53
    assert result["today_count"] == 61
    assert result["today_request_total"] == 61
    assert result["today_success"] == 39
    assert result["today_failed"] == 14
    assert result["today_success_rate"] == 73.6
