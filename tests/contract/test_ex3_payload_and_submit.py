from datetime import UTC, datetime

from contracts.schemas import Ex3CandidateGraphDelta
from subsystem_sdk.backends import MockSubmitBackend
from subsystem_sdk.submit import SubmitClient

from subsystem_holdings.public import build_default_offline_producer


def test_ex3_payloads_validate_against_contract() -> None:
    producer = build_default_offline_producer()
    result = producer.build_payloads(
        produced_at=datetime(2026, 3, 31, 8, 0, tzinfo=UTC)
    )

    assert len(result.payloads) == 2
    for payload in result.payloads:
        wire_payload = {
            key: value
            for key, value in payload.items()
            if key not in {"ex_type", "produced_at"}
        }
        Ex3CandidateGraphDelta.model_validate(wire_payload)


def test_mock_submit_backend_receives_valid_wire_payloads() -> None:
    backend = MockSubmitBackend()
    client = SubmitClient(backend)
    producer = build_default_offline_producer()

    result = producer.submit(
        client,
        produced_at=datetime(2026, 3, 31, 8, 0, tzinfo=UTC),
    )

    assert len(result.receipts) == 2
    assert len(backend.submitted_payloads) == 2
    relation_types = {payload["relation_type"] for payload in backend.submitted_payloads}
    assert relation_types == {"CO_HOLDING", "NORTHBOUND_HOLD"}
    for payload in backend.submitted_payloads:
        assert "ex_type" not in payload
        assert "produced_at" not in payload
        Ex3CandidateGraphDelta.model_validate(payload)
