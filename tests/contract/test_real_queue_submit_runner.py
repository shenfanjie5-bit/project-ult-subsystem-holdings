from __future__ import annotations

import importlib.util
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

import pytest

from subsystem_holdings.models import ProducerResult
from subsystem_holdings.public import build_default_offline_producer


def _load_runner() -> Any:
    root = Path(__file__).resolve().parents[2]
    scripts_dir = root / "scripts"
    if str(scripts_dir) not in sys.path:
        sys.path.insert(0, str(scripts_dir))
    spec = importlib.util.spec_from_file_location(
        "proof_real_queue_submit_path",
        scripts_dir / "proof_real_queue_submit_path.py",
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
    audit: tuple[Any, ...] = ()

    def build_payloads(self) -> ProducerResult:
        return ProducerResult(payloads=self.payloads, audit=self.audit)


def _duckdb_placeholder(tmp_path: Path) -> Path:
    path = tmp_path / "verified.duckdb"
    path.write_bytes(b"not-opened-by-test")
    return path


def _valid_payloads() -> tuple[dict[str, Any], ...]:
    result = build_default_offline_producer().build_payloads()
    return tuple(dict(payload) for payload in result.payloads)


def test_execute_without_gate_fails_closed_before_submit(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = _load_runner()

    def fail_if_called(_: Path) -> tuple[_FakeProducer, _FakeAdapter]:
        raise AssertionError("runner must not read marts without live submit gate")

    monkeypatch.setattr(runner, "build_read_only_producer", fail_if_called)

    with pytest.raises(runner.ProofRunnerError) as error:
        runner.run_real_queue_submit_proof(
            _duckdb_placeholder(tmp_path),
            execute=True,
            env={},
            submit_candidate_func=lambda _: {"id": "must-not-submit"},
        )

    assert error.value.reason == "missing_live_queue_submit_confirmation"


def test_dry_run_summary_is_sanitized_and_does_not_submit(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = _load_runner()
    payloads = _valid_payloads()
    monkeypatch.setattr(
        runner,
        "build_read_only_producer",
        lambda _: (_FakeProducer(payloads), _FakeAdapter()),
    )

    summary = runner.run_real_queue_submit_proof(
        _duckdb_placeholder(tmp_path),
        execute=False,
        submit_candidate_func=lambda _: pytest.fail("dry-run must not submit"),
    )
    summary_path = tmp_path / "summary.json"
    runner._write_summary(summary_path, summary)
    summary_text = summary_path.read_text(encoding="utf-8")

    assert summary["mode"] == "dry_run"
    assert summary["submitted"] is False
    assert summary["duckdb_path"] == "<redacted>"
    assert summary["payload_count"] == 2
    assert summary["receipt_count"] == 0
    assert summary["relation_counts"] == {"CO_HOLDING": 1, "NORTHBOUND_HOLD": 1}
    assert str(tmp_path) not in summary_text


def test_execute_with_gate_uses_sdk_queue_backend_and_sanitized_envelope(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = _load_runner()
    payloads = _valid_payloads()
    monkeypatch.setattr(
        runner,
        "build_read_only_producer",
        lambda _: (_FakeProducer(payloads), _FakeAdapter()),
    )
    captured: list[dict[str, Any]] = []

    def record_submit_candidate(payload: Mapping[str, Any]) -> Mapping[str, Any]:
        captured.append(dict(payload))
        return {"id": f"candidate-{len(captured)}"}

    summary = runner.run_real_queue_submit_proof(
        _duckdb_placeholder(tmp_path),
        execute=True,
        env={runner.CONFIRM_ENV: "1"},
        submit_candidate_func=record_submit_candidate,
    )

    assert summary["mode"] == "execute"
    assert summary["submitted"] is True
    assert summary["payload_count"] == 2
    assert summary["accepted_receipt_count"] == 2
    assert summary["receipt_backend_kinds"] == ["data_platform_queue"]
    assert len(captured) == 2
    assert {payload["relation_type"] for payload in captured} == {
        "CO_HOLDING",
        "NORTHBOUND_HOLD",
    }

    forbidden = {
        "ex_type",
        "produced_at",
        "submitted_at",
        "ingest_seq",
        "layer_b_receipt_id",
    }
    for envelope in captured:
        assert envelope["payload_type"] == "Ex-3"
        assert envelope["submitted_by"] == "subsystem-holdings"
        assert forbidden.isdisjoint(envelope)


def test_disallowed_relation_type_fails_before_submit(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = _load_runner()
    payload = dict(_valid_payloads()[0])
    payload["relation_type"] = "TOP_SHAREHOLDER"
    captured: list[dict[str, Any]] = []
    monkeypatch.setattr(
        runner,
        "build_read_only_producer",
        lambda _: (_FakeProducer((payload,)), _FakeAdapter()),
    )

    with pytest.raises(runner.ProofRunnerError) as error:
        runner.run_real_queue_submit_proof(
            _duckdb_placeholder(tmp_path),
            execute=True,
            env={runner.CONFIRM_ENV: "1"},
            submit_candidate_func=lambda payload: captured.append(dict(payload)),
        )

    assert error.value.reason == "disallowed_relation_type"
    assert captured == []


def test_resolves_duckdb_path_from_command_scoped_env(tmp_path: Path) -> None:
    runner = _load_runner()
    path = _duckdb_placeholder(tmp_path)

    assert runner.resolve_duckdb_path(None, {runner.DUCKDB_PATH_ENV: str(path)}) == path
