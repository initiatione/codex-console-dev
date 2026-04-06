from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def read_text(*parts: str) -> str:
    return (ROOT.joinpath(*parts)).read_text(encoding="utf-8")


def test_email_services_template_exposes_duckduckgo_cloudmail_fields():
    template = read_text("templates", "email_services.html")

    assert 'value="duckduckgo_cloudmail"' in template
    assert 'id="add-duckduckgo-cloudmail-fields"' in template
    assert 'name="ddgc_bridge_base_url"' in template
    assert 'name="ddgc_bridge_token"' in template
    assert 'name="ddgc_cloudmail_service_id"' in template
    assert 'name="ddgc_forward_to_email"' in template
    assert 'name="ddgc_pool_target_size"' in template
    assert 'name="ddgc_low_watermark"' in template
    assert 'id="edit-duckduckgo-cloudmail-fields"' in template


def test_registration_template_exposes_duckduckgo_cloudmail_choices():
    template = read_text("templates", "index.html")

    assert template.count('value="duckduckgo_cloudmail"') >= 2
    assert 'DuckDuckGo-CloudMail' in template


def test_frontend_scripts_wire_duckduckgo_cloudmail_type():
    email_services_js = read_text("static", "js", "email_services.js")
    app_js = read_text("static", "js", "app.js")
    utils_js = read_text("static", "js", "utils.js")

    assert "addDuckduckgoCloudmailFields" in email_services_js
    assert "editDuckduckgoCloudmailFields" in email_services_js
    assert "service_type=duckduckgo_cloudmail" in email_services_js
    assert "serviceType = 'duckduckgo_cloudmail'" in email_services_js
    assert "availableServices.duckduckgo_cloudmail" in app_js
    assert "duckduckgo_cloudmail:${service.id}" in app_js
    assert "duckduckgo_cloudmail: 'DuckDuckGo-CloudMail'" in utils_js
