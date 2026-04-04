from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_email_services_page_has_direct_enable_disable_buttons():
    content = (ROOT / "static" / "js" / "email_services.js").read_text(encoding="utf-8")

    assert "btn ${service.enabled ? 'btn-warning' : 'btn-success'} btn-sm" in content
    assert "toggleService(${service.id}, ${!service.enabled})\">${service.enabled ? '禁用' : '启用'}" in content
    assert "testService(${service.id})\">测试</button>" in content


def test_email_services_template_expands_operation_column_for_direct_actions():
    content = (ROOT / "templates" / "email_services.html").read_text(encoding="utf-8")

    assert content.count('<th style="width: 240px;">操作</th>') >= 2