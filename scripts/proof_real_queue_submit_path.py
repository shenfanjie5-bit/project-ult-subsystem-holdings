#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from subsystem_holdings.entity_registry_adapter import EntityRegistryAdapter
from subsystem_holdings.mart_adapter import AdapterDiagnostic
from subsystem_holdings.submit_client import build_data_platform_queue_submit_client

from proof_queue_submit_path import build_read_only_producer

CONFIRM_ENV = "SUBSYSTEM_HOLDINGS_LIVE_QUEUE_SUBMIT_CONFIRM"
DUCKDB_PATH_ENV = "DP_DUCKDB_PATH"
ALLOWED_RELATION_TYPES = frozenset({"CO_HOLDING", "NORTHBOUND_HOLD"})
PRIVATE_WIRE_FIELDS = frozenset(
    {
        "payload_type",
        "submitted_by",
        "submitted_at",
        "ingest_seq",
        "layer_b_receipt_id",
    }
)


@dataclass(frozen=True, slots=True)
class ProofRunnerError(RuntimeError):
    reason: str
    exit_code: int

    def __str__(self) -> str:
        return self.reason


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be greater than zero")
    return parsed


def resolve_duckdb_path(
    cli_path: Path | None,
    env: Mapping[str, str] | None = None,
) -> Path:
    source = cli_path
    if source is None:
        value = (env or os.environ).get(DUCKDB_PATH_ENV)
        if value:
            source = Path(value)
    if source is None:
        raise ProofRunnerError("duckdb_path_missing", 2)
    return source


def _diagnostic_summary(diagnostics: Sequence[AdapterDiagnostic]) -> list[dict[str, str]]:
    return [
        {
            "table": diagnostic.table,
            "reason": diagnostic.reason,
        }
        for diagnostic in diagnostics
    ]


def _relation_counts(payloads: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    return dict(Counter(str(payload.get("relation_type")) for payload in payloads))


def _private_wire_field_leaks(payloads: Sequence[Mapping[str, Any]]) -> list[str]:
    return sorted(
        {
            field
            for payload in payloads
            for field in PRIVATE_WIRE_FIELDS.intersection(payload)
        }
    )


def _validate_payload_selection(payloads: Sequence[Mapping[str, Any]]) -> None:
    if not payloads:
        raise ProofRunnerError("no_payloads", 3)

    relation_types = {str(payload.get("relation_type")) for payload in payloads}
    if relation_types.difference(ALLOWED_RELATION_TYPES):
        raise ProofRunnerError("disallowed_relation_type", 4)

    if _private_wire_field_leaks(payloads):
        raise ProofRunnerError("private_wire_field_leak", 4)


def _receipt_summary(receipts: Sequence[Any]) -> dict[str, Any]:
    return {
        "receipt_count": len(receipts),
        "accepted_receipt_count": sum(
            1 for receipt in receipts if bool(getattr(receipt, "accepted", False))
        ),
        "rejected_receipt_count": sum(
            1 for receipt in receipts if not bool(getattr(receipt, "accepted", False))
        ),
        "receipt_backend_kinds": sorted(
            {str(getattr(receipt, "backend_kind", "")) for receipt in receipts}
        ),
        "receipt_error_count": sum(
            len(tuple(getattr(receipt, "errors", ()) or ())) for receipt in receipts
        ),
        "receipt_transport_ref_count": sum(
            1 for receipt in receipts if getattr(receipt, "transport_ref", None)
        ),
    }


def run_real_queue_submit_proof(
    duckdb_path: Path,
    *,
    execute: bool = False,
    max_payloads: int | None = None,
    env: Mapping[str, str] | None = None,
    submit_candidate_func: Any | None = None,
    entity_lookup: Any | None = None,
) -> dict[str, Any]:
    runtime_env = env or os.environ
    if execute and runtime_env.get(CONFIRM_ENV) != "1":
        raise ProofRunnerError("missing_live_queue_submit_confirmation", 6)

    if not duckdb_path.exists():
        raise ProofRunnerError("duckdb_path_missing", 2)

    registry_adapter = EntityRegistryAdapter()
    producer, adapter = build_read_only_producer(
        duckdb_path,
        aligner=registry_adapter,
    )
    result = producer.build_payloads()
    selected_payloads = tuple(result.payloads[:max_payloads])
    _validate_payload_selection(selected_payloads)

    receipts: tuple[Any, ...] = ()
    if execute:
        client = build_data_platform_queue_submit_client(
            submit_candidate_func=submit_candidate_func,
            entity_lookup=entity_lookup or registry_adapter,
            entity_preflight_profile="production",
        )
        receipts = tuple(client.submit(payload) for payload in selected_payloads)

    relation_counts = _relation_counts(selected_payloads)
    summary: dict[str, Any] = {
        "proof": "holdings_real_queue_submit_path",
        "mode": "execute" if execute else "dry_run",
        "submitted": execute,
        "duckdb_path": "<redacted>",
        "payload_count": len(selected_payloads),
        "built_payload_count": len(result.payloads),
        "audit_count": len(result.audit),
        "relation_counts": relation_counts,
        "relation_type_set": sorted(relation_counts),
        "disallowed_relation_types": sorted(
            set(relation_counts).difference(ALLOWED_RELATION_TYPES)
        ),
        "private_wire_field_leaks": _private_wire_field_leaks(selected_payloads),
        "adapter_diagnostics": _diagnostic_summary(adapter.diagnostics),
    }
    summary.update(_receipt_summary(receipts))
    return summary


def _exit_code_from_summary(summary: Mapping[str, Any]) -> int:
    if summary["payload_count"] == 0:
        return 3
    if summary["disallowed_relation_types"] or summary["private_wire_field_leaks"]:
        return 4
    if summary["submitted"] and (
        summary["receipt_count"] != summary["payload_count"]
        or summary["accepted_receipt_count"] != summary["payload_count"]
    ):
        return 5
    return 0


def _write_summary(path: Path, summary: Mapping[str, Any]) -> None:
    path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Holdings Ex-3 real data-platform queue submit proof runner. "
            "Defaults to dry-run summary; real submit requires --execute and "
            f"{CONFIRM_ENV}=1."
        ),
    )
    parser.add_argument(
        "--duckdb-path",
        type=Path,
        default=None,
        help=f"Path to verified holdings DuckDB marts. Defaults to {DUCKDB_PATH_ENV}.",
    )
    parser.add_argument(
        "--max-payloads",
        type=_positive_int,
        default=None,
        help="Optional positive cap on payloads submitted or summarized.",
    )
    parser.add_argument(
        "--summary-json",
        type=Path,
        default=None,
        help="Optional path for sanitized summary JSON.",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Submit to the real data-platform queue backend.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    try:
        duckdb_path = resolve_duckdb_path(args.duckdb_path)
        summary = run_real_queue_submit_proof(
            duckdb_path,
            execute=args.execute,
            max_payloads=args.max_payloads,
        )
    except ProofRunnerError as exc:
        summary = {
            "proof": "holdings_real_queue_submit_path",
            "status": "failed",
            "reason": exc.reason,
            "duckdb_path": "<redacted>",
        }
        if args.summary_json is not None:
            _write_summary(args.summary_json, summary)
        print(json.dumps(summary, indent=2, sort_keys=True))
        return exc.exit_code

    if args.summary_json is not None:
        _write_summary(args.summary_json, summary)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return _exit_code_from_summary(summary)


if __name__ == "__main__":
    raise SystemExit(main())
