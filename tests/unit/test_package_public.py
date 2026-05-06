from subsystem_holdings import __version__
from subsystem_holdings.public import (
    build_default_offline_producer,
    build_mock_submit_client,
)


def test_package_import_and_public_api() -> None:
    assert __version__ == "0.1.0"
    producer = build_default_offline_producer()
    result = producer.build_payloads()
    assert {payload["relation_type"] for payload in result.payloads} == {
        "CO_HOLDING",
        "NORTHBOUND_HOLD",
    }


def test_mock_submit_client_builder() -> None:
    client = build_mock_submit_client()
    assert client.backend.backend_kind == "mock"
