#!/usr/bin/env python3
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from subsystem_holdings.entity_registry_adapter import EntityRegistryAdapter
from subsystem_holdings.errors import AdapterSchemaError
from subsystem_holdings.mart_adapter import (
    FUND_CO_HOLDING_LINEAGE_MART,
    FUND_CO_HOLDING_MART,
    NORTHBOUND_Z_SCORE_LINEAGE_MART,
    NORTHBOUND_Z_SCORE_MART,
    TOP_HOLDER_QOQ_LINEAGE_MART,
    TOP_HOLDER_QOQ_MART,
    AdapterDiagnostic,
    ReadOnlyMartAdapter,
)
from subsystem_holdings.models import AuditRecord, ProducerResult
from subsystem_holdings.producer import HoldingsProducer
from subsystem_holdings.submit_client import build_data_platform_queue_submit_client
from subsystem_sdk.validate.preflight import EntityRegistryLookup, run_entity_preflight

CONFIRM_ENV = "SUBSYSTEM_HOLDINGS_PRODUCTION_QUEUE_SUBMIT_CONFIRM"
DUCKDB_PATH_ENV = "DP_DUCKDB_PATH"

Mode = Literal["readiness", "execute"]

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
SUBMIT_DIAGNOSTIC_TABLES = frozenset(
    {
        FUND_CO_HOLDING_MART,
        FUND_CO_HOLDING_LINEAGE_MART,
        NORTHBOUND_Z_SCORE_MART,
        NORTHBOUND_Z_SCORE_LINEAGE_MART,
    }
)
TOP_HOLDER_DIAGNOSTIC_TABLES = frozenset(
    {
        TOP_HOLDER_QOQ_MART,
        TOP_HOLDER_QOQ_LINEAGE_MART,
    }
)


@dataclass(frozen=True, slots=True)
class ProductionRunnerError(RuntimeError):
    reason: str
    exit_code: int

    def __str__(self) -> str:
        return self.reason


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be greater than zero")
    return parsed


def _non_negative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be zero or greater")
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
        raise ProductionRunnerError("duckdb_path_missing", 2)
    return source


def build_read_only_producer(
    duckdb_path: Path,
    *,
    registry_adapter: EntityRegistryAdapter | None = None,
) -> tuple[HoldingsProducer, ReadOnlyMartAdapter]:
    adapter = ReadOnlyMartAdapter.from_duckdb_path(duckdb_path)
    adapter.clear_diagnostics()
    return HoldingsProducer(adapter, registry_adapter or EntityRegistryAdapter()), adapter


def _recording_submit_candidate(
    captured_count: list[int],
) -> Any:
    def submit_candidate(_: Mapping[str, Any]) -> Mapping[str, Any]:
        captured_count[0] += 1
        return {"id": f"production-readiness-noop-{captured_count[0]}"}

    return submit_candidate


def _counter_dict(values: Iterable[str]) -> dict[str, int]:
    return dict(Counter(values))


def _audit_counts(audit: Sequence[AuditRecord]) -> dict[str, int]:
    return _counter_dict(record.reason for record in audit)


def _diagnostic_counts(
    diagnostics: Sequence[AdapterDiagnostic],
) -> list[dict[str, object]]:
    counter = Counter((diagnostic.table, diagnostic.reason) for diagnostic in diagnostics)
    return [
        {"table": table, "reason": reason, "count": count}
        for (table, reason), count in sorted(counter.items())
    ]


def _diagnostic_count_for(
    diagnostics: Sequence[AdapterDiagnostic],
    tables: frozenset[str],
) -> int:
    return sum(1 for diagnostic in diagnostics if diagnostic.table in tables)


def _relation_counts(payloads: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    return _counter_dict(str(payload.get("relation_type")) for payload in payloads)


def _private_wire_field_leaks(payloads: Sequence[Mapping[str, Any]]) -> list[str]:
    return sorted(
        {
            field
            for payload in payloads
            for field in PRIVATE_WIRE_FIELDS.intersection(payload)
        }
    )


def _receipt_summary(receipts: Sequence[Any], *, prefix: str = "") -> dict[str, Any]:
    return {
        f"{prefix}receipt_count": len(receipts),
        f"{prefix}accepted_receipt_count": sum(
            1 for receipt in receipts if bool(getattr(receipt, "accepted", False))
        ),
        f"{prefix}rejected_receipt_count": sum(
            1 for receipt in receipts if not bool(getattr(receipt, "accepted", False))
        ),
        f"{prefix}receipt_backend_kinds": sorted(
            {str(getattr(receipt, "backend_kind", "")) for receipt in receipts}
        ),
        f"{prefix}receipt_error_count": sum(
            len(tuple(getattr(receipt, "errors", ()) or ())) for receipt in receipts
        ),
        f"{prefix}receipt_warning_count": sum(
            len(tuple(getattr(receipt, "warnings", ()) or ())) for receipt in receipts
        ),
        f"{prefix}receipt_transport_ref_count": sum(
            1 for receipt in receipts if getattr(receipt, "transport_ref", None)
        ),
    }


def _production_preflight_summary(
    payloads: Sequence[Mapping[str, Any]],
    *,
    lookup: EntityRegistryLookup,
) -> dict[str, int]:
    checked = 0
    blocked = 0
    unresolved = 0
    warnings = 0
    for payload in payloads:
        preflight = run_entity_preflight(
            payload,
            lookup=lookup,
            policy="block",
            lookup_unavailable_policy="fail",
        )
        if preflight.checked:
            checked += 1
        if preflight.should_block:
            blocked += 1
        unresolved += len(preflight.unresolved_refs)
        warnings += len(preflight.warnings)
    return {
        "preflight_payload_count": len(payloads),
        "preflight_checked_count": checked,
        "preflight_blocked_count": blocked,
        "preflight_unresolved_ref_count": unresolved,
        "preflight_warning_count": warnings,
    }


def _stable_payload_view(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _stable_payload_view(item)
            for key, item in sorted(value.items())
            if key != "produced_at"
        }
    if isinstance(value, list | tuple):
        return [_stable_payload_view(item) for item in value]
    return value


def stable_run_id(payloads: Sequence[Mapping[str, Any]]) -> str:
    payload_view = _stable_payload_view(payloads)
    digest = hashlib.sha256(
        json.dumps(payload_view, sort_keys=True, separators=(",", ":"), default=str).encode(
            "utf-8"
        )
    ).hexdigest()
    return f"holdings-prod-{digest[:16]}"


def _failure_run_id(reason: str) -> str:
    digest = hashlib.sha256(reason.encode("utf-8")).hexdigest()
    return f"holdings-prod-{digest[:16]}"


def _select_payloads(
    payloads: Sequence[Mapping[str, Any]],
    *,
    mode: Mode,
    max_payloads: int | None,
    allow_partial_submit: bool,
) -> tuple[Mapping[str, Any], ...]:
    if max_payloads is None:
        return tuple(payloads)
    if mode == "execute" and not allow_partial_submit:
        raise ProductionRunnerError("partial_execute_not_allowed", 7)
    return tuple(payloads[:max_payloads])


def _readiness_reasons(
    *,
    payload_count: int,
    unresolved_alignment_count: int,
    max_unresolved_alignments: int,
    submit_mart_diagnostic_count: int,
    max_submit_mart_diagnostics: int,
    disallowed_relation_types: Sequence[str],
    private_wire_field_leaks: Sequence[str],
    preflight_blocked_count: int,
    preflight_accepted_receipt_count: int,
    preflight_receipt_count: int,
) -> tuple[str, ...]:
    reasons: list[str] = []
    if payload_count == 0:
        reasons.append("no_payloads")
    if unresolved_alignment_count > max_unresolved_alignments:
        reasons.append("unresolved_alignments_exceeded")
    if submit_mart_diagnostic_count > max_submit_mart_diagnostics:
        reasons.append("submit_mart_diagnostics_exceeded")
    if disallowed_relation_types:
        reasons.append("disallowed_relation_type")
    if private_wire_field_leaks:
        reasons.append("private_wire_field_leak")
    if preflight_blocked_count:
        reasons.append("production_preflight_blocked")
    if preflight_receipt_count != payload_count:
        reasons.append("preflight_receipt_count_mismatch")
    if preflight_accepted_receipt_count != payload_count:
        reasons.append("preflight_receipt_rejected")
    return tuple(reasons)


def _schema_failure_summary(
    *,
    mode: Mode,
    run_id: str | None,
    adapter: Any,
) -> dict[str, Any]:
    diagnostics = tuple(getattr(adapter, "diagnostics", ()) or ())
    summary: dict[str, Any] = {
        "run_id": run_id or _failure_run_id("adapter_schema_mismatch"),
        "mode": mode,
        "ready": False,
        "submitted": False,
        "reason": "adapter_schema_mismatch",
        "payload_count": 0,
        "built_payload_count": 0,
        "selected_payload_count": 0,
        "relation_counts": {},
        "audit_counts": {},
        "adapter_diagnostic_counts": _diagnostic_counts(diagnostics),
        "submit_mart_diagnostic_count": _diagnostic_count_for(
            diagnostics,
            SUBMIT_DIAGNOSTIC_TABLES,
        ),
        "top_holder_diagnostic_count": _diagnostic_count_for(
            diagnostics,
            TOP_HOLDER_DIAGNOSTIC_TABLES,
        ),
        "idempotent_safe_receipt_supported": False,
        "submit_backend_limitations": ["sdk_data_platform_queue_idempotent_not_exposed"],
    }
    summary.update(_receipt_summary((), prefix="preflight_"))
    summary.update(_receipt_summary(()))
    return summary


def _build_readiness_summary(
    *,
    mode: Mode,
    run_id: str | None,
    payloads: tuple[Mapping[str, Any], ...],
    selected_payloads: tuple[Mapping[str, Any], ...],
    audit: Sequence[AuditRecord],
    diagnostics: Sequence[AdapterDiagnostic],
    preflight_counts: Mapping[str, int],
    preflight_receipts: Sequence[Any],
    max_unresolved_alignments: int,
    max_submit_mart_diagnostics: int,
    partial_submit_allowed: bool,
) -> dict[str, Any]:
    audit_counts = _audit_counts(audit)
    unresolved_alignment_count = audit_counts.get(
        "unresolved_holder", 0
    ) + audit_counts.get("unresolved_security", 0)
    submit_mart_diagnostic_count = _diagnostic_count_for(
        diagnostics,
        SUBMIT_DIAGNOSTIC_TABLES,
    )
    top_holder_diagnostic_count = _diagnostic_count_for(
        diagnostics,
        TOP_HOLDER_DIAGNOSTIC_TABLES,
    )
    relation_counts = _relation_counts(payloads)
    disallowed_relation_types = sorted(set(relation_counts).difference(ALLOWED_RELATION_TYPES))
    private_wire_field_leaks = _private_wire_field_leaks(payloads)
    preflight_receipt_summary = _receipt_summary(preflight_receipts, prefix="preflight_")
    reasons = _readiness_reasons(
        payload_count=len(payloads),
        unresolved_alignment_count=unresolved_alignment_count,
        max_unresolved_alignments=max_unresolved_alignments,
        submit_mart_diagnostic_count=submit_mart_diagnostic_count,
        max_submit_mart_diagnostics=max_submit_mart_diagnostics,
        disallowed_relation_types=disallowed_relation_types,
        private_wire_field_leaks=private_wire_field_leaks,
        preflight_blocked_count=int(preflight_counts["preflight_blocked_count"]),
        preflight_accepted_receipt_count=int(
            preflight_receipt_summary["preflight_accepted_receipt_count"]
        ),
        preflight_receipt_count=int(preflight_receipt_summary["preflight_receipt_count"]),
    )

    summary: dict[str, Any] = {
        "run_id": run_id or stable_run_id(payloads),
        "mode": mode,
        "ready": not reasons,
        "submitted": False,
        "payload_count": len(payloads),
        "built_payload_count": len(payloads),
        "selected_payload_count": len(selected_payloads),
        "partial_submit_allowed": partial_submit_allowed,
        "relation_counts": relation_counts,
        "relation_type_set": sorted(relation_counts),
        "disallowed_relation_type_count": len(disallowed_relation_types),
        "private_wire_field_leak_count": len(private_wire_field_leaks),
        "audit_counts": audit_counts,
        "unresolved_alignment_count": unresolved_alignment_count,
        "adapter_diagnostic_counts": _diagnostic_counts(diagnostics),
        "submit_mart_diagnostic_count": submit_mart_diagnostic_count,
        "top_holder_diagnostic_count": top_holder_diagnostic_count,
        "idempotent_safe_receipt_supported": False,
        "submit_backend_limitations": ["sdk_data_platform_queue_idempotent_not_exposed"],
    }
    if reasons:
        summary["reason"] = reasons[0]
        summary["reasons"] = list(reasons)
    summary.update(preflight_counts)
    summary.update(preflight_receipt_summary)
    summary.update(_receipt_summary(()))
    return summary


def run_production_queue_submit(
    duckdb_path: Path,
    *,
    mode: Mode = "readiness",
    max_payloads: int | None = None,
    allow_partial_submit: bool = False,
    max_unresolved_alignments: int = 0,
    max_submit_mart_diagnostics: int = 0,
    run_id: str | None = None,
    env: Mapping[str, str] | None = None,
    submit_candidate_func: Any | None = None,
    entity_lookup: EntityRegistryLookup | None = None,
) -> dict[str, Any]:
    runtime_env = env or os.environ
    if mode == "execute" and runtime_env.get(CONFIRM_ENV) != "1":
        raise ProductionRunnerError("missing_production_queue_submit_confirmation", 6)
    if mode == "execute" and max_payloads is not None and not allow_partial_submit:
        raise ProductionRunnerError("partial_execute_not_allowed", 7)
    if not duckdb_path.exists():
        raise ProductionRunnerError("duckdb_path_missing", 2)

    registry_adapter = entity_lookup or EntityRegistryAdapter()
    producer, adapter = build_read_only_producer(
        duckdb_path,
        registry_adapter=registry_adapter
        if isinstance(registry_adapter, EntityRegistryAdapter)
        else None,
    )
    try:
        result: ProducerResult = producer.build_payloads()
    except AdapterSchemaError:
        return _schema_failure_summary(mode=mode, run_id=run_id, adapter=adapter)

    # Keep run-id generation and dry-run validation side-effect-free for callers.
    payloads = tuple(copy.deepcopy(dict(payload)) for payload in result.payloads)
    selected_payloads = _select_payloads(
        payloads,
        mode=mode,
        max_payloads=max_payloads,
        allow_partial_submit=allow_partial_submit,
    )
    lookup = entity_lookup or registry_adapter
    preflight_counts = _production_preflight_summary(payloads, lookup=lookup)

    captured_count = [0]
    readiness_client = build_data_platform_queue_submit_client(
        submit_candidate_func=_recording_submit_candidate(captured_count),
        entity_lookup=lookup,
        entity_preflight_profile="production",
    )
    preflight_receipts = tuple(readiness_client.submit(payload) for payload in payloads)

    summary = _build_readiness_summary(
        mode=mode,
        run_id=run_id,
        payloads=payloads,
        selected_payloads=selected_payloads,
        audit=result.audit,
        diagnostics=tuple(adapter.diagnostics),
        preflight_counts=preflight_counts,
        preflight_receipts=preflight_receipts,
        max_unresolved_alignments=max_unresolved_alignments,
        max_submit_mart_diagnostics=max_submit_mart_diagnostics,
        partial_submit_allowed=allow_partial_submit,
    )
    if mode == "readiness" or not summary["ready"]:
        return summary

    selected_preflight = _production_preflight_summary(selected_payloads, lookup=lookup)
    if selected_preflight["preflight_blocked_count"]:
        summary["ready"] = False
        summary["reason"] = "selected_production_preflight_blocked"
        summary["selected_preflight_blocked_count"] = selected_preflight[
            "preflight_blocked_count"
        ]
        return summary

    submit_client = build_data_platform_queue_submit_client(
        submit_candidate_func=submit_candidate_func,
        entity_lookup=lookup,
        entity_preflight_profile="production",
    )
    receipts = tuple(submit_client.submit(payload) for payload in selected_payloads)
    summary["submitted"] = True
    summary.update(_receipt_summary(receipts))
    if (
        summary["receipt_count"] != len(selected_payloads)
        or summary["accepted_receipt_count"] != len(selected_payloads)
    ):
        summary["ready"] = False
        summary["reason"] = "submit_receipt_mismatch"
    return summary


def _exit_code_from_summary(summary: Mapping[str, Any]) -> int:
    if not bool(summary.get("ready")):
        if summary.get("reason") == "submit_receipt_mismatch":
            return 5
        return 4
    if summary.get("mode") == "execute" and (
        summary.get("receipt_count") != summary.get("selected_payload_count")
        or summary.get("accepted_receipt_count") != summary.get("selected_payload_count")
    ):
        return 5
    return 0


def _write_summary(path: Path, summary: Mapping[str, Any]) -> None:
    path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Production holdings Ex-3 queue submit runner. Defaults to readiness; "
            f"execute requires {CONFIRM_ENV}=1."
        ),
    )
    parser.add_argument(
        "--duckdb-path",
        type=Path,
        default=None,
        help=f"Path to verified holdings DuckDB marts. Defaults to {DUCKDB_PATH_ENV}.",
    )
    parser.add_argument(
        "--mode",
        choices=("readiness", "execute"),
        default="readiness",
        help="Run full production readiness or execute after readiness passes.",
    )
    parser.add_argument(
        "--max-payloads",
        type=_positive_int,
        default=None,
        help=(
            "Optional cap on selected payloads. Execute requires "
            "--allow-partial-submit."
        ),
    )
    parser.add_argument(
        "--allow-partial-submit",
        action="store_true",
        help="Allow execute canary submission when --max-payloads is set.",
    )
    parser.add_argument(
        "--max-unresolved-alignments",
        type=_non_negative_int,
        default=0,
        help="Maximum unresolved holder/security alignments allowed for readiness.",
    )
    parser.add_argument(
        "--max-submit-mart-diagnostics",
        type=_non_negative_int,
        default=0,
        help="Maximum blocking submit-mart adapter diagnostics allowed for readiness.",
    )
    parser.add_argument(
        "--summary-json",
        type=Path,
        default=None,
        help="Optional path for sanitized summary JSON.",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Optional external run id. Defaults to a stable payload hash.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    try:
        duckdb_path = resolve_duckdb_path(args.duckdb_path)
        summary = run_production_queue_submit(
            duckdb_path,
            mode=args.mode,
            max_payloads=args.max_payloads,
            allow_partial_submit=args.allow_partial_submit,
            max_unresolved_alignments=args.max_unresolved_alignments,
            max_submit_mart_diagnostics=args.max_submit_mart_diagnostics,
            run_id=args.run_id,
        )
    except ProductionRunnerError as exc:
        summary = {
            "run_id": args.run_id or _failure_run_id(exc.reason),
            "mode": args.mode,
            "ready": False,
            "submitted": False,
            "reason": exc.reason,
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
