import importlib


def test_runtime_role_defaults_to_api(monkeypatch) -> None:
    monkeypatch.delenv("EA_RUNTIME_ROLE", raising=False)
    module = importlib.import_module("app.service.runtime_role")
    module = importlib.reload(module)

    assert module.get_runtime_role() == "api"
    assert module.role_enabled("api") is True
    assert module.role_enabled("scheduler") is False
    assert module.role_enabled("worker") is False


def test_runtime_role_invalid_value_falls_back_to_api(monkeypatch) -> None:
    monkeypatch.setenv("EA_RUNTIME_ROLE", "all")
    module = importlib.import_module("app.service.runtime_role")
    module = importlib.reload(module)

    assert module.get_runtime_role() == "api"
    assert module.role_enabled("api") is True
    assert module.role_enabled("worker") is False
