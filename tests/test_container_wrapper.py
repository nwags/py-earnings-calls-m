from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _read(path: str) -> str:
    return (REPO_ROOT / path).read_text(encoding="utf-8")


def test_dockerfile_default_runtime_command_targets_service_runtime_api():
    dockerfile = _read("Dockerfile")
    assert 'CMD ["python", "-m", "py_earnings_calls.service_runtime", "api", "--host", "0.0.0.0", "--port", "8000"]' in dockerfile


def test_compose_command_and_default_mounts_are_runtime_data_only():
    compose = _read("docker-compose.yml")
    assert 'command: ["python", "-m", "py_earnings_calls.service_runtime", "api", "--host", "0.0.0.0", "--port", "8000"]' in compose
    assert "- ./.earnings_cache:/workspace/.earnings_cache" in compose
    assert "- ./refdata:/workspace/refdata" in compose
    assert "- ./data:/workspace/data" in compose
    assert "./:/workspace" not in compose
    assert ".:/workspace" not in compose
    assert "env_file:" not in compose


def test_docs_reference_runtime_surface_and_container_examples():
    readme = _read("README.md")
    usage = _read("docs/usage.rst")
    runtime = _read("py_earnings_calls/service_runtime.py")

    assert '@main.command("api")' in runtime
    assert '@main.command("monitor-once")' in runtime
    assert '@main.command("monitor-loop")' in runtime

    assert "python -m py_earnings_calls.service_runtime api --host 0.0.0.0 --port 8000" in readme
    assert "python -m py_earnings_calls.service_runtime monitor-once" in readme
    assert "python -m py_earnings_calls.service_runtime monitor-loop" in readme
    assert "docker compose up api" in readme

    assert "python -m py_earnings_calls.service_runtime api --host 0.0.0.0 --port 8000" in usage
    assert "python -m py_earnings_calls.service_runtime monitor-once" in usage
    assert "python -m py_earnings_calls.service_runtime monitor-loop" in usage
    assert "docker compose up api" in usage
