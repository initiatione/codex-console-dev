from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_settings_page_contains_codex2api_service_management_ui():
    content = (ROOT / "templates" / "settings.html").read_text(encoding="utf-8")
    assert "add-codex2api-service-btn" in content
    assert "codex2api-services-table" in content
    assert "codex2api-service-edit-modal" in content


def test_index_page_contains_codex2api_auto_upload_ui():
    content = (ROOT / "templates" / "index.html").read_text(encoding="utf-8")
    assert "auto-upload-codex2api" in content
    assert "codex2api-service-select-group" in content
    assert "codex2api-service-select" in content


def test_accounts_page_contains_codex2api_upload_entry():
    content = (ROOT / "templates" / "accounts.html").read_text(encoding="utf-8")
    assert "batch-upload-codex2api-item" in content
    assert "codex2api-service-modal" in content
