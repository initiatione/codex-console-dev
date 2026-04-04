from fastapi import FastAPI

from src.web.routes.upload.codex2api_services import router


def test_codex2api_router_registers_expected_paths():
    app = FastAPI()
    app.include_router(router, prefix="/codex2api-services")

    paths = {route.path for route in app.routes}

    assert "/codex2api-services" in paths
    assert "/codex2api-services/{service_id}" in paths
    assert "/codex2api-services/{service_id}/full" in paths
    assert "/codex2api-services/{service_id}/test" in paths
    assert "/codex2api-services/test-connection" in paths
    assert "/codex2api-services/upload" in paths
