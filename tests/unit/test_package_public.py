import tomllib
from pathlib import Path

from subsystem_holdings import __version__
from subsystem_holdings.public import (
    EntityAlignmentResolver,
    EntityRegistryAdapter,
    build_default_offline_producer,
    build_mock_submit_client,
)


def test_package_import_and_public_api() -> None:
    assert __version__ == "0.1.0"
    assert EntityAlignmentResolver is not None
    assert EntityRegistryAdapter is not None
    producer = build_default_offline_producer()
    result = producer.build_payloads()
    assert {payload["relation_type"] for payload in result.payloads} == {
        "CO_HOLDING",
        "NORTHBOUND_HOLD",
    }


def test_mock_submit_client_builder() -> None:
    client = build_mock_submit_client()
    assert client.backend.backend_kind == "mock"


def test_documented_duckdb_helper_has_runtime_dependency() -> None:
    pyproject_path = Path(__file__).resolve().parents[2] / "pyproject.toml"
    pyproject = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))

    dependencies = pyproject["project"]["dependencies"]
    assert any(dependency.startswith("duckdb>=") for dependency in dependencies)
