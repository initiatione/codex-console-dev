from fastapi import BackgroundTasks

from src.web.routes import registration


def test_dispatch_registration_config_maps_codex2api_fields_for_single(monkeypatch):
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
                "auto_upload_codex2api": True,
                "codex2api_service_ids": [9],
                "filter_only_access_token_accounts": False,
            }
        )
    )

    assert result["kind"] == "single"
    assert captured["request"].auto_upload_codex2api is True
    assert captured["request"].codex2api_service_ids == [9]
    assert captured["request"].filter_only_access_token_accounts is False


def test_dispatch_registration_config_maps_codex2api_fields_for_batch(monkeypatch):
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
                "auto_upload_codex2api": True,
                "codex2api_service_ids": [3, 4],
                "filter_only_access_token_accounts": False,
            }
        )
    )
    assert result["kind"] == "batch"
    assert captured["request"].auto_upload_codex2api is True
    assert captured["request"].codex2api_service_ids == [3, 4]
    assert captured["request"].filter_only_access_token_accounts is False


def test_start_batch_registration_internal_passes_codex2api_args(monkeypatch):
    captured = {}

    def fake_schedule(background_tasks, coroutine_func, *args):
        captured["background_tasks"] = background_tasks
        captured["coroutine_func"] = coroutine_func
        captured["args"] = args

    monkeypatch.setattr(registration, "_schedule_async_job", fake_schedule)
    monkeypatch.setattr(registration, "_validate_registration_request", lambda _: None)
    monkeypatch.setattr(registration.uuid, "uuid4", lambda: "fixed-uuid")

    fake_task = type(
        "Task",
        (),
        {
            "id": 1,
            "task_uuid": "fixed-uuid",
            "status": "pending",
            "email_service_id": None,
            "proxy": None,
            "logs": None,
            "result": None,
            "error_message": None,
            "created_at": None,
            "started_at": None,
            "completed_at": None,
        },
    )()

    class FakeDb:
        pass

    class FakeContext:
        def __enter__(self):
            return FakeDb()

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(registration, "get_db", lambda: FakeContext())
    monkeypatch.setattr(registration.crud, "create_registration_task", lambda db, task_uuid, proxy: None)
    monkeypatch.setattr(registration.crud, "get_registration_task", lambda db, item_uuid: fake_task)

    request = registration.BatchRegistrationRequest(
        count=1,
        email_service_type="tempmail",
        auto_upload_codex2api=True,
        codex2api_service_ids=[11, 12],
        filter_only_access_token_accounts=False,
    )

    response = registration.asyncio.run(
        registration._start_batch_registration_internal(request, BackgroundTasks())
    )

    assert response.batch_id == "fixed-uuid"
    assert captured["coroutine_func"] is registration.run_batch_registration
    assert captured["args"][14] is True
    assert captured["args"][15] == [11, 12]
    assert captured["args"][20] is False
