from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest
from contracts.schemas import Ex3CandidateGraphDelta

from subsystem_holdings.public import build_default_offline_producer
from subsystem_holdings import submit_client as submit_client_module
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


def test_data_platform_queue_submit_client_uses_idempotent_config_true(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: dict[str, Any] = {}

    def fake_build_submit_backend(config: Any, **kwargs: Any) -> Any:
        seen["data_platform_idempotent_required"] = (
            config.data_platform_idempotent_required
        )
        seen["idempotent_func"] = kwargs.get(
            "data_platform_submit_candidate_idempotent"
        )
        return SimpleNamespace(
            backend_kind="data_platform_queue",
            _idempotent_required=config.data_platform_idempotent_required,
            submit=lambda _payload: {
                "accepted": True,
                "transport_ref": "fake",
                "warnings": (),
                "errors": (),
            },
        )

    idempotent_func = lambda payload: {"id": payload["delta_id"]}  # noqa: E731
    monkeypatch.setattr(
        submit_client_module,
        "build_submit_backend",
        fake_build_submit_backend,
    )

    build_data_platform_queue_submit_client(
        submit_candidate_idempotent_func=idempotent_func,
        idempotent_required=True,
    )

    assert seen == {
        "data_platform_idempotent_required": True,
        "idempotent_func": idempotent_func,
    }


def test_data_platform_queue_idempotent_safe_receipt_passthrough_is_sanitized() -> None:
    class SafeReceipt:
        candidate_id = 321
        replayed = True

        def as_public_dict(self) -> dict[str, Any]:
            return {
                "candidate_id": self.candidate_id,
                "submitted_at": "2026-03-31T08:00:00+00:00",
                "ingest_seq": 456,
                "validation_status": "pending",
                "rejection_reason": None,
                "replayed": self.replayed,
                "payload": {"provider_payload": "must-not-leak"},
                "raw_payload_path": "/tmp/private.json",
            }

    captured: list[dict[str, Any]] = []

    def submit_candidate_idempotent(payload: Mapping[str, Any]) -> SafeReceipt:
        captured.append(dict(payload))
        return SafeReceipt()

    client = build_data_platform_queue_submit_client(
        submit_candidate_idempotent_func=submit_candidate_idempotent,
        idempotent_required=True,
    )
    payload = build_default_offline_producer().build_payloads().payloads[0]
    receipt = client.submit(payload)

    assert receipt.accepted is True
    assert receipt.backend_kind == "data_platform_queue"
    assert receipt.transport_ref == "321"
    assert "data_platform_queue idempotent replay" in receipt.warnings
    assert receipt.errors == ()
    assert captured[0]["payload_type"] == "Ex-3"
    assert captured[0]["submitted_by"] == "subsystem-holdings"
    assert {"ex_type", "produced_at", "submitted_at", "ingest_seq"}.isdisjoint(
        captured[0]
    )
    receipt_text = str(receipt)
    assert "candidate_id" not in receipt_text
    assert "provider_payload" not in receipt_text
    assert "/tmp/private.json" not in receipt_text
