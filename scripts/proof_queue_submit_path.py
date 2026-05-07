#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import Counter
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from subsystem_holdings.alignment import EntityAligner, EntityAlignmentTable
from subsystem_holdings.mart_adapter import ReadOnlyMartAdapter
from subsystem_holdings.producer import HoldingsProducer
from subsystem_holdings.reader import HoldingsMartReader
from subsystem_holdings.submit_client import build_data_platform_queue_submit_client

ALLOWED_RELATION_TYPES = {"CO_HOLDING", "NORTHBOUND_HOLD"}
RESERVED_ENVELOPE_FIELDS = {
    "ex_type",
    "produced_at",
    "submitted_at",
    "ingest_seq",
    "layer_b_receipt_id",
}


def _normalize_node_id(prefix: str, source_id: str) -> str:
    normalized = "".join(
        character if character.isalnum() else "_"
        for character in source_id.strip().upper()
    ).strip("_")
    return f"{prefix}_{normalized or 'UNRESOLVED'}"


def build_proof_only_aligner(reader: HoldingsMartReader) -> EntityAligner:
    holder_nodes: dict[str, str] = {}
    security_nodes: dict[str, str] = {}

    for row in reader.canonical_positions():
        holder_nodes[row.holder_id] = _normalize_node_id("ENT_HOLDER", row.holder_id)
        security_nodes[row.security_id] = _normalize_node_id(
            "ENT_SECURITY",
            row.security_id,
        )

    for row in reader.fund_co_holdings():
        security_nodes[row.security_id_left] = _normalize_node_id(
            "ENT_SECURITY",
            row.security_id_left,
        )
        security_nodes[row.security_id_right] = _normalize_node_id(
            "ENT_SECURITY",
            row.security_id_right,
        )

    for row in reader.northbound_z_scores():
        holder_nodes[row.holder_id] = _normalize_node_id("ENT_HOLDER", row.holder_id)
        security_nodes[row.security_id] = _normalize_node_id(
            "ENT_SECURITY",
            row.security_id,
        )

    return EntityAligner(
        EntityAlignmentTable(
            holder_nodes=holder_nodes,
            security_nodes=security_nodes,
        )
    )


def build_read_only_producer(
    duckdb_path: Path,
    aligner: EntityAligner | None = None,
) -> tuple[HoldingsProducer, ReadOnlyMartAdapter]:
    adapter = ReadOnlyMartAdapter.from_duckdb_path(duckdb_path)
    proof_aligner = aligner if aligner is not None else build_proof_only_aligner(adapter)
    adapter.clear_diagnostics()
    return HoldingsProducer(adapter, proof_aligner), adapter


def _recording_submit_candidate(
    captured: list[dict[str, Any]],
) -> Any:
    def submit_candidate(payload: Mapping[str, Any]) -> Mapping[str, Any]:
        captured.append(dict(payload))
        return {"id": f"proof-candidate-{len(captured)}"}

    return submit_candidate


def _diagnostic_summary(adapter: ReadOnlyMartAdapter) -> list[dict[str, str]]:
    return [
        {
            "table": diagnostic.table,
            "reason": diagnostic.reason,
        }
        for diagnostic in adapter.diagnostics
    ]


def _relation_counts(envelopes: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    return dict(Counter(str(envelope.get("relation_type")) for envelope in envelopes))


def run_proof(duckdb_path: Path) -> dict[str, Any]:
    captured: list[dict[str, Any]] = []
    producer, adapter = build_read_only_producer(duckdb_path)
    client = build_data_platform_queue_submit_client(
        submit_candidate_func=_recording_submit_candidate(captured)
    )

    result = producer.submit(client)
    relation_types = {str(envelope.get("relation_type")) for envelope in captured}
    reserved_field_leaks = sorted(
        {
            field
            for envelope in captured
            for field in RESERVED_ENVELOPE_FIELDS.intersection(envelope)
        }
    )
    disallowed_relation_types = sorted(relation_types.difference(ALLOWED_RELATION_TYPES))

    return {
        "proof": "holdings_queue_submit_path",
        "mode": "proof_only_injected_recorder",
        "duckdb_path": "<redacted>",
        "payload_count": len(result.payloads),
        "audit_count": len(result.audit),
        "captured_envelope_count": len(captured),
        "relation_counts": _relation_counts(captured),
        "payload_type_set": sorted(
            {str(envelope.get("payload_type")) for envelope in captured}
        ),
        "submitted_by_set": sorted(
            {str(envelope.get("submitted_by")) for envelope in captured}
        ),
        "receipt_backend_kinds": sorted(
            {str(receipt.backend_kind) for receipt in result.receipts}
        ),
        "accepted_receipt_count": sum(
            1 for receipt in result.receipts if receipt.accepted
        ),
        "reserved_field_leaks": reserved_field_leaks,
        "disallowed_relation_types": disallowed_relation_types,
        "adapter_diagnostics": _diagnostic_summary(adapter),
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Proof-only holdings Ex-3 queue submit path with injected recorder.",
    )
    parser.add_argument(
        "--duckdb-path",
        required=True,
        type=Path,
        help="Path to a data-platform DuckDB file containing holdings marts.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    summary = run_proof(args.duckdb_path)
    print(json.dumps(summary, indent=2, sort_keys=True))

    if summary["payload_count"] == 0:
        return 2
    if summary["payload_count"] != summary["captured_envelope_count"]:
        return 3
    if summary["reserved_field_leaks"] or summary["disallowed_relation_types"]:
        return 4
    if summary["accepted_receipt_count"] != summary["payload_count"]:
        return 5
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
