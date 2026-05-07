from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from contracts.schemas import Ex3CandidateGraphDelta

from subsystem_holdings.public import build_default_offline_producer
from subsystem_holdings.submit_client import build_data_platform_queue_submit_client


class _PreflightLookup:
    def __init__(self, resolved_refs: set[str] | None = None) -> None:
        self._resolved_refs = resolved_refs or set()
        self.calls: list[tuple[str, ...]] = []

    def lookup(self, refs):
        refs_tuple = tuple(refs)
        self.calls.append(refs_tuple)
        return {ref: ref in self._resolved_refs for ref in refs_tuple}


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


def test_queue_submit_entity_preflight_block_rejects_before_backend_call() -> None:
    captured: list[dict[str, Any]] = []

    def record_submit_candidate(payload: Mapping[str, Any]) -> Mapping[str, Any]:
        captured.append(dict(payload))
        return {"id": "must-not-submit"}

    payload = build_default_offline_producer().build_payloads().payloads[0]
    lookup = _PreflightLookup(resolved_refs={"ENT_SECURITY_ALPHA"})
    client = build_data_platform_queue_submit_client(
        submit_candidate_func=record_submit_candidate,
        entity_lookup=lookup,
        preflight_policy="block",
    )

    receipt = client.submit(payload)

    assert captured == []
    assert receipt.accepted is False
    assert receipt.backend_kind == "data_platform_queue"
    assert receipt.errors == (
        "entity preflight blocked unresolved reference(s): ENT_SECURITY_BETA",
    )
    assert (
        "entity preflight found unresolved reference(s): ENT_SECURITY_BETA"
        in receipt.warnings
    )
    assert lookup.calls == [("ENT_SECURITY_ALPHA", "ENT_SECURITY_BETA")]
