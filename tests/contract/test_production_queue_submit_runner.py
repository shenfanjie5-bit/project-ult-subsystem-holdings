from __future__ import annotations

import copy
import importlib.util
import sys
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping

import pytest

from subsystem_holdings.errors import AdapterSchemaError
from subsystem_holdings.mart_adapter import (
    FUND_CO_HOLDING_MART,
    TOP_HOLDER_QOQ_MART,
    AdapterDiagnostic,
)
from subsystem_holdings.models import AuditRecord, ProducerResult
from subsystem_holdings.public import build_default_offline_producer


def _load_runner() -> Any:
    root = Path(__file__).resolve().parents[2]
    scripts_dir = root / "scripts"
    if str(scripts_dir) not in sys.path:
        sys.path.insert(0, str(scripts_dir))
    spec = importlib.util.spec_from_file_location(
        "run_production_queue_submit",
        scripts_dir / "run_production_queue_submit.py",
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@dataclass(slots=True)
class _FakeAdapter:
    diagnostics: list[Any] = field(default_factory=list)


@dataclass(slots=True)
class _FakeProducer:
    payloads: tuple[dict[str, Any], ...]
    audit: tuple[AuditRecord, ...] = ()

    def build_payloads(self) -> ProducerResult:
        return ProducerResult(payloads=self.payloads, audit=self.audit)


@dataclass(slots=True)
class _SchemaFailProducer:
    def build_payloads(self) -> ProducerResult:
        raise AdapterSchemaError("raw_secret_path /tmp/private missing column")


@dataclass(slots=True)
class _EntityLookup:
    missing: set[str] = field(default_factory=set)
    calls: list[tuple[str, ...]] = field(default_factory=list)

    def lookup(self, refs: Any) -> Mapping[str, bool]:
        refs_tuple = tuple(refs)
        self.calls.append(refs_tuple)
        return {ref: ref not in self.missing for ref in refs_tuple}


def _duckdb_placeholder(tmp_path: Path) -> Path:
    path = tmp_path / "verified.duckdb"
    path.write_bytes(b"not-opened-by-test")
    return path


def _valid_payloads() -> tuple[dict[str, Any], ...]:
    result = build_default_offline_producer().build_payloads()
    return tuple(dict(payload) for payload in result.payloads)


def test_readiness_ready_summary_is_sanitized(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = _load_runner()
    payloads = _valid_payloads()
    monkeypatch.setattr(
        runner,
        "build_read_only_producer",
        lambda *_args, **_kwargs: (_FakeProducer(payloads), _FakeAdapter()),
    )

    summary = runner.run_production_queue_submit(
        _duckdb_placeholder(tmp_path),
        mode="readiness",
        entity_lookup=_EntityLookup(),
    )
    summary_path = tmp_path / "summary.json"
    runner._write_summary(summary_path, summary)
    summary_text = summary_path.read_text(encoding="utf-8")

    assert summary["ready"] is True
    assert summary["submitted"] is False
    assert summary["payload_count"] == 2
    assert summary["preflight_accepted_receipt_count"] == 2
    assert summary["receipt_count"] == 0
    assert summary["relation_counts"] == {"CO_HOLDING": 1, "NORTHBOUND_HOLD": 1}
    assert summary["idempotent_safe_receipt_supported"] is True
    assert summary["submit_backend_limitations"] == []
    assert str(tmp_path) not in summary_text
    assert "delta_id" not in summary_text
    assert "source_node" not in summary_text


def test_execute_missing_preflight_ref_does_not_call_real_backend(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = _load_runner()
    payloads = _valid_payloads()
    monkeypatch.setattr(
        runner,
        "build_read_only_producer",
        lambda *_args, **_kwargs: (_FakeProducer(payloads), _FakeAdapter()),
    )
    real_submit_calls: list[Mapping[str, Any]] = []

    summary = runner.run_production_queue_submit(
        _duckdb_placeholder(tmp_path),
        mode="execute",
        env={runner.CONFIRM_ENV: "1"},
        entity_lookup=_EntityLookup(missing={"ENT_SECURITY_BETA"}),
        submit_candidate_func=lambda payload: real_submit_calls.append(dict(payload)),
    )

    assert summary["ready"] is False
    assert summary["submitted"] is False
    assert summary["reason"] == "production_preflight_blocked"
    assert summary["preflight_blocked_count"] == 1
    assert real_submit_calls == []


def test_submit_mart_missing_lineage_blocks_readiness(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = _load_runner()
    payloads = _valid_payloads()
    adapter = _FakeAdapter(
        diagnostics=[
            AdapterDiagnostic(
                table=FUND_CO_HOLDING_MART,
                reason="missing_lineage",
                detail="derivation row has no paired lineage row",
                row_key="raw-secret-row",
            )
        ]
    )
    monkeypatch.setattr(
        runner,
        "build_read_only_producer",
        lambda *_args, **_kwargs: (_FakeProducer(payloads), adapter),
    )

    summary = runner.run_production_queue_submit(
        _duckdb_placeholder(tmp_path),
        entity_lookup=_EntityLookup(),
    )

    assert summary["ready"] is False
    assert summary["reason"] == "submit_mart_diagnostics_exceeded"
    assert summary["submit_mart_diagnostic_count"] == 1
    assert summary["adapter_diagnostic_counts"] == [
        {"table": FUND_CO_HOLDING_MART, "reason": "missing_lineage", "count": 1}
    ]
    assert "raw-secret-row" not in str(summary)


def test_schema_mismatch_failure_summary_is_sanitized(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = _load_runner()
    adapter = _FakeAdapter(
        diagnostics=[
            AdapterDiagnostic(
                table=FUND_CO_HOLDING_MART,
                reason="schema_mismatch",
                detail="raw_secret_path /tmp/private missing column",
            )
        ]
    )
    monkeypatch.setattr(
        runner,
        "build_read_only_producer",
        lambda *_args, **_kwargs: (_SchemaFailProducer(), adapter),
    )

    summary = runner.run_production_queue_submit(
        _duckdb_placeholder(tmp_path),
        entity_lookup=_EntityLookup(),
    )

    summary_text = str(summary)
    assert summary["ready"] is False
    assert summary["reason"] == "adapter_schema_mismatch"
    assert summary["payload_count"] == 0
    assert "raw_secret_path" not in summary_text
    assert "/tmp/private" not in summary_text
    assert str(tmp_path) not in summary_text


def test_top_holder_diagnostics_and_skip_audit_do_not_block_by_default(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = _load_runner()
    payloads = _valid_payloads()
    adapter = _FakeAdapter(
        diagnostics=[
            AdapterDiagnostic(
                table=TOP_HOLDER_QOQ_MART,
                reason="missing_lineage",
                detail="top-holder lineage unavailable",
                row_key="top-secret-row",
            )
        ]
    )
    audit = (
        AuditRecord(
            row_id="top-secret-row",
            reason="read_only_input",
            detail={"message": "top-holder skipped"},
        ),
    )
    monkeypatch.setattr(
        runner,
        "build_read_only_producer",
        lambda *_args, **_kwargs: (_FakeProducer(payloads, audit), adapter),
    )

    summary = runner.run_production_queue_submit(
        _duckdb_placeholder(tmp_path),
        entity_lookup=_EntityLookup(),
    )

    assert summary["ready"] is True
    assert summary["top_holder_diagnostic_count"] == 1
    assert summary["audit_counts"] == {"read_only_input": 1}
    assert "top-secret-row" not in str(summary)


def test_generated_run_id_is_stable_and_does_not_mutate_payloads(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = _load_runner()
    payloads = _valid_payloads()
    original_payloads = copy.deepcopy(payloads)
    monkeypatch.setattr(
        runner,
        "build_read_only_producer",
        lambda *_args, **_kwargs: (_FakeProducer(payloads), _FakeAdapter()),
    )

    first = runner.run_production_queue_submit(
        _duckdb_placeholder(tmp_path),
        entity_lookup=_EntityLookup(),
    )
    second = runner.run_production_queue_submit(
        _duckdb_placeholder(tmp_path),
        entity_lookup=_EntityLookup(),
    )

    assert first["run_id"] == second["run_id"]
    assert payloads == original_payloads


def test_execute_gate_fails_closed_before_reading_marts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = _load_runner()

    def fail_if_called(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("execute gate must be checked before mart reads")

    monkeypatch.setattr(runner, "build_read_only_producer", fail_if_called)

    with pytest.raises(runner.ProductionRunnerError) as error:
        runner.run_production_queue_submit(
            _duckdb_placeholder(tmp_path),
            mode="execute",
            env={},
            entity_lookup=_EntityLookup(),
            submit_candidate_func=lambda _: {"id": "must-not-submit"},
        )

    assert error.value.reason == "missing_production_queue_submit_confirmation"


def test_execute_success_requires_all_selected_receipts_accepted(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = _load_runner()
    payloads = _valid_payloads()
    monkeypatch.setattr(
        runner,
        "build_read_only_producer",
        lambda *_args, **_kwargs: (_FakeProducer(payloads), _FakeAdapter()),
    )
    captured: list[dict[str, Any]] = []

    def record_submit_candidate_idempotent(
        payload: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        captured.append(dict(payload))
        return {"id": f"candidate-{len(captured)}"}

    summary = runner.run_production_queue_submit(
        _duckdb_placeholder(tmp_path),
        mode="execute",
        env={runner.CONFIRM_ENV: "1"},
        entity_lookup=_EntityLookup(),
        submit_candidate_idempotent_func=record_submit_candidate_idempotent,
    )

    assert summary["ready"] is True
    assert summary["submitted"] is True
    assert summary["idempotent_safe_receipt_supported"] is True
    assert summary["submit_backend_limitations"] == []
    assert summary["receipt_count"] == 2
    assert summary["accepted_receipt_count"] == 2
    assert summary["receipt_backend_kinds"] == ["data_platform_queue"]
    assert len(captured) == 2
    assert {"ex_type", "produced_at", "submitted_at", "ingest_seq"}.isdisjoint(
        captured[0]
    )


def test_execute_missing_sdk_idempotent_api_fails_closed_before_submit(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = _load_runner()
    payloads = _valid_payloads()
    monkeypatch.setattr(
        runner,
        "build_read_only_producer",
        lambda *_args, **_kwargs: (_FakeProducer(payloads), _FakeAdapter()),
    )

    from subsystem_sdk.backends import data_platform_queue as queue_backend_module

    legacy_calls: list[dict[str, Any]] = []

    def legacy_submit_candidate(payload: Mapping[str, Any]) -> Mapping[str, Any]:
        legacy_calls.append(dict(payload))
        return {"id": "legacy-must-not-submit"}

    monkeypatch.setattr(
        queue_backend_module,
        "import_module",
        lambda _name: SimpleNamespace(submit_candidate=legacy_submit_candidate),
    )

    summary = runner.run_production_queue_submit(
        _duckdb_placeholder(tmp_path),
        mode="execute",
        env={runner.CONFIRM_ENV: "1"},
        entity_lookup=_EntityLookup(),
        submit_candidate_func=legacy_submit_candidate,
    )

    assert summary["ready"] is False
    assert summary["submitted"] is False
    assert summary["reason"] == runner.IDEMPOTENT_BACKEND_LIMITATION
    assert summary["idempotent_safe_receipt_supported"] is False
    assert summary["submit_backend_limitations"] == [runner.IDEMPOTENT_BACKEND_LIMITATION]
    assert summary["receipt_count"] == 0
    assert legacy_calls == []


def test_execute_idempotent_safe_receipt_summary_is_sanitized(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = _load_runner()
    payloads = _valid_payloads()
    monkeypatch.setattr(
        runner,
        "build_read_only_producer",
        lambda *_args, **_kwargs: (_FakeProducer(payloads), _FakeAdapter()),
    )

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
                "payload": {"delta_id": "private-delta"},
                "raw_payload_path": "/tmp/private.json",
            }

    captured: list[dict[str, Any]] = []

    def idempotent_submit(payload: Mapping[str, Any]) -> SafeReceipt:
        captured.append(dict(payload))
        return SafeReceipt()

    summary = runner.run_production_queue_submit(
        _duckdb_placeholder(tmp_path),
        mode="execute",
        max_payloads=1,
        allow_partial_submit=True,
        env={runner.CONFIRM_ENV: "1"},
        entity_lookup=_EntityLookup(),
        submit_candidate_idempotent_func=idempotent_submit,
    )

    assert summary["ready"] is True
    assert summary["submitted"] is True
    assert summary["selected_payload_count"] == 1
    assert summary["receipt_count"] == 1
    assert summary["accepted_receipt_count"] == 1
    assert summary["receipt_warning_count"] >= 1
    assert summary["receipt_transport_ref_count"] == 1
    assert len(captured) == 1
    assert captured[0]["payload_type"] == "Ex-3"
    assert captured[0]["submitted_by"] == "subsystem-holdings"
    assert {"ex_type", "produced_at", "submitted_at", "ingest_seq"}.isdisjoint(
        captured[0]
    )
    assert "private-delta" not in str(summary)
    assert "/tmp/private.json" not in str(summary)


def test_execute_partial_submit_requires_explicit_allowance(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = _load_runner()

    def fail_if_called(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("partial execute gate must run before mart reads")

    monkeypatch.setattr(runner, "build_read_only_producer", fail_if_called)

    with pytest.raises(runner.ProductionRunnerError) as error:
        runner.run_production_queue_submit(
            _duckdb_placeholder(tmp_path),
            mode="execute",
            max_payloads=1,
            env={runner.CONFIRM_ENV: "1"},
            entity_lookup=_EntityLookup(),
            submit_candidate_idempotent_func=lambda _: {"id": "must-not-submit"},
        )

    assert error.value.reason == "partial_execute_not_allowed"
