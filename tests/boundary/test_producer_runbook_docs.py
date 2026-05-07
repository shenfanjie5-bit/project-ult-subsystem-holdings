from __future__ import annotations

from pathlib import Path


def test_producer_runbook_documents_required_operational_guards() -> None:
    root = Path(__file__).resolve().parents[2]
    runbook = (root / "docs" / "producer-runbook.md").read_text(encoding="utf-8")
    readme = (root / "README.md").read_text(encoding="utf-8")

    required_markers = (
        "existing `Ex3CandidateGraphDelta`",
        "`CO_HOLDING`",
        "`NORTHBOUND_HOLD`",
        "`OWNERSHIP` properties",
        "`--entity-registry-fixture`",
        "`--entity-registry-alias-map`",
        "`--scope-manifest`",
        "two-hop context",
        "graph/risk context",
        "`scope_filtered_payload_count`",
        "`delta_id`",
        "idempotent data-platform queue API",
        "`audit_counts`",
        "`adapter_diagnostic_counts`",
        "fail closed",
    )

    for marker in required_markers:
        assert marker in runbook

    assert "docs/producer-runbook.md" in readme


def test_producer_runbook_does_not_claim_forbidden_rollout_state() -> None:
    root = Path(__file__).resolve().parents[2]
    text = (root / "docs" / "producer-runbook.md").read_text(encoding="utf-8").lower()

    forbidden_claims = (
        "default propagation enabled",
        "full propagation enabled",
        "broad rollout complete",
        "broad production rollout complete",
    )

    for claim in forbidden_claims:
        assert claim not in text
