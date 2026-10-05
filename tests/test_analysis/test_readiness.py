"""Cold datasets and failed stores must never advertise a ready deployment."""

import stat

import pytest
from fastapi.testclient import TestClient

from backend import readiness
from backend.main import app
from scripts import demo


def test_empty_dataset_is_not_ready(tmp_path, monkeypatch):
    monkeypatch.setattr(readiness.settings, "data_dir", tmp_path)
    response = TestClient(app).get("/api/readiness")
    assert response.status_code == 503
    assert response.json()["checks"] == {"corpus": False, "graph": False, "index": False}
    assert TestClient(app).get("/api/health").status_code == 200


def test_store_failure_does_not_expose_connection_secret(monkeypatch):
    def fail(*args):
        raise RuntimeError("secret-password@private-host")

    monkeypatch.setattr(readiness, "load_verified_corpus", fail)
    response = TestClient(app).get("/api/readiness")
    assert response.status_code == 503
    assert "secret-password" not in response.text


def test_credentials_persist_with_private_permissions(tmp_path):
    path = tmp_path / "private/demo.env"
    first = demo.prepare_secret(path)
    assert demo.prepare_secret(path) == first
    assert len(first["NEO4J_PASSWORD"]) >= 32
    assert stat.S_IMODE(path.stat().st_mode) == 0o600


def test_credentials_cannot_override_other_environment(tmp_path):
    path = tmp_path / "demo.env"
    path.write_text("NEO4J_PASSWORD=existing-password\nDOCKER_HOST=untrusted\n")
    with pytest.raises(ValueError, match="only NEO4J_PASSWORD"):
        demo.prepare_secret(path)


def test_status_does_not_create_credentials(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(demo, "ROOT", tmp_path)
    monkeypatch.setattr(demo, "status", lambda: {"status": "not_ready"})
    demo.run_demo("status")
    assert not (tmp_path / "data").exists()
    assert "not_ready" in capsys.readouterr().out


def test_stale_graph_is_not_ready(tmp_path, monkeypatch):
    import json
    from contextlib import nullcontext

    monkeypatch.setattr(readiness.settings, "data_dir", tmp_path)
    monkeypatch.setattr(readiness, "load_verified_corpus", lambda *args: {"matches": []})
    folder = tmp_path / "reports/mvp" / readiness.settings.season
    folder.mkdir(parents=True)
    (folder / "quality.json").write_text(json.dumps({"artifact_sha256": {"matches": "current"}}))
    (folder / "stores.json").write_text(
        json.dumps({"valid": True, "graph": {"counts": {"Match": 380}}})
    )

    class Driver:
        def session(self):
            return nullcontext(object())

    monkeypatch.setattr(readiness, "connect", lambda: nullcontext(Driver()))
    monkeypatch.setattr(
        readiness, "query", lambda *args: [{"label": "Match", "count": 380, "versions": ["old"]}]
    )
    response = TestClient(app).get("/api/readiness")
    assert response.status_code == 503
    assert response.json()["checks"] == {"corpus": True, "graph": False, "index": False}


def test_public_client_preserves_login_and_cleans_up(tmp_path, monkeypatch):
    import json
    import os
    from pathlib import Path

    original = {
        "currentContext": "desktop-linux",
        "credsStore": "desktop",
        "auths": {"private": {}},
    }
    (tmp_path / "config.json").write_text(json.dumps(original))
    context = tmp_path / "contexts/meta/example"
    context.mkdir(parents=True)
    (context / "meta.json").write_text('{"Name":"desktop-linux"}')
    monkeypatch.setenv("DOCKER_CONFIG", str(tmp_path))
    with demo.public_image_client():
        isolated = Path(os.environ["DOCKER_CONFIG"])
        assert json.loads((isolated / "config.json").read_text()) == {
            "currentContext": "desktop-linux"
        }
        assert (isolated / "contexts/meta/example/meta.json").exists()
    assert not isolated.exists()
    assert os.environ["DOCKER_CONFIG"] == str(tmp_path)
    assert json.loads((tmp_path / "config.json").read_text()) == original
