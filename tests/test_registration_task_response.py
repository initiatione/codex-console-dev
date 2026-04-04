from src.database.models import Base, EmailService, RegistrationTask
from src.database.session import DatabaseSessionManager
from src.web.routes.registration import (
    RECOVERED_PENDING_TASK_ERROR,
    RECOVERED_RUNNING_TASK_ERROR,
    task_to_response,
)


def test_task_to_response_marks_startup_recovered_running_task():
    service = EmailService(id=15, name='测试云邮', service_type='cloudmail', config={})
    task = RegistrationTask(
        id=101,
        task_uuid='task-recovered-running-001',
        status='failed',
        email_service_id=15,
        error_message=RECOVERED_RUNNING_TASK_ERROR,
    )
    task.email_service = service

    response = task_to_response(task)

    assert response.recovered_on_startup is True
    assert response.recovery_reason == 'startup_recovered_running'
    assert response.email_service_name == '测试云邮'
    assert response.email_service_type == 'cloudmail'


def test_task_to_response_marks_startup_recovered_pending_task():
    task = RegistrationTask(
        id=102,
        task_uuid='task-recovered-pending-001',
        status='cancelled',
        error_message=RECOVERED_PENDING_TASK_ERROR,
    )

    response = task_to_response(task)

    assert response.recovered_on_startup is True
    assert response.recovery_reason == 'startup_recovered_pending'


def test_task_to_response_keeps_normal_task_unmarked():
    task = RegistrationTask(
        id=103,
        task_uuid='task-normal-001',
        status='failed',
        error_message='普通失败',
    )

    response = task_to_response(task)

    assert response.recovered_on_startup is False
    assert response.recovery_reason is None


def test_task_to_response_handles_detached_task_without_lazy_loading(tmp_path):
    db_path = tmp_path / 'task_response_detached.db'
    manager = DatabaseSessionManager(f'sqlite:///{db_path}')
    Base.metadata.create_all(bind=manager.engine)

    session = manager.SessionLocal()
    try:
        service = EmailService(name='脱离会话邮箱', service_type='outlook', config={})
        session.add(service)
        session.commit()
        session.refresh(service)
        service_id = service.id

        task = RegistrationTask(
            task_uuid='task-detached-001',
            status='pending',
            email_service_id=service_id,
        )
        session.add(task)
        session.commit()
        session.refresh(task)

        detached_task = session.query(RegistrationTask).filter(RegistrationTask.id == task.id).first()
        assert detached_task is not None
        assert 'email_service' not in detached_task.__dict__
        session.expunge(detached_task)
    finally:
        session.close()
        manager.engine.dispose()

    response = task_to_response(detached_task)

    assert response.email_service_id == service_id
    assert response.email_service_name is None
    assert response.email_service_type is None
    assert response.task_uuid == 'task-detached-001'


def test_task_to_response_prefers_explicit_email_service_snapshot():
    task = RegistrationTask(
        id=104,
        task_uuid='task-explicit-snapshot-001',
        status='pending',
        email_service_id=88,
    )

    response = task_to_response(
        task,
        email_service_name='显式邮箱服务',
        email_service_type='outlook',
    )

    assert response.email_service_id == 88
    assert response.email_service_name == '显式邮箱服务'
    assert response.email_service_type == 'outlook'
