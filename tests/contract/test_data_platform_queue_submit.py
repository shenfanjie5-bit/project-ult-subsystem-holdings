from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from contracts.schemas import Ex3CandidateGraphDelta

from subsystem_holdings.public import build_default_offline_producer
from subsystem_holdings.submit_client import build_data_platform_queue_submit_client


def test_data_platform_queue_submit_client_captures_sanitized_ex3_envelope() -> None:
    captured: list[dict[str, Any]] = []

    def record_submit_candidate(payload: Mapping[str, Any]) -> Mapping[str, Any]:
        captured.append(dict(payload))
        return {"id": f"candidate-{len(captured)}"}

    client = build_data_platform_queue_submit_client(
        submit_candidate_func=record_submit_candidate
    )
    result = build_default_offline_producer().submit(
        client,
        produced_at=datetime(2026, 3, 31, 8, 0, tzinfo=UTC),
    )

    assert len(result.payloads) == 2
    assert len(result.receipts) == 2
    assert len(captured) == 2
    assert {receipt.backend_kind for receipt in result.receipts} == {
        "data_platform_queue"
    }
    assert all(receipt.accepted for receipt in result.receipts)
    assert [receipt.transport_ref for receipt in result.receipts] == [
        "candidate-1",
        "candidate-2",
    ]

    relation_types = {envelope["relation_type"] for envelope in captured}
    assert relation_types == {"CO_HOLDING", "NORTHBOUND_HOLD"}

    for envelope in captured:
        assert envelope["payload_type"] == "Ex-3"
        assert envelope["submitted_by"] == "subsystem-holdings"
        assert set(envelope).isdisjoint(
            {
                "ex_type",
                "produced_at",
                "submitted_at",
                "ingest_seq",
                "layer_b_receipt_id",
            }
        )
        Ex3CandidateGraphDelta.model_validate(
            {
                key: value
                for key, value in envelope.items()
                if key not in {"payload_type", "submitted_by"}
            }
        )
