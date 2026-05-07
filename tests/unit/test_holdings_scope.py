from __future__ import annotations

from datetime import UTC, datetime

import pytest
from contracts.schemas import Ex3CandidateGraphDelta

from subsystem_holdings.alignment import EntityAligner, EntityAlignmentTable
from subsystem_holdings.errors import ScopeManifestError
from subsystem_holdings.models import FundCoHoldingRow, LineageSummary, NorthboundZScoreRow
from subsystem_holdings.producer import HoldingsProducer
from subsystem_holdings.reader import FakeHoldingsMartReader
from subsystem_holdings.scope import HoldingsScope, holdings_scope_from_manifest


def test_scope_manifest_accepts_target_and_two_hop_context_refs() -> None:
    scope = holdings_scope_from_manifest(
        {
            "holdings_scope": {
                "target_entities": [{"ref": "ENT_SECURITY_ALPHA"}],
                "two_hop_context": [
                    {"entity_id": "ENT_SECURITY_BETA"},
                    "ENT_SECURITY_GAMMA",
                ],
            },
        }
    )

    assert scope.target_entity_refs == frozenset({"ENT_SECURITY_ALPHA"})
    assert scope.two_hop_context_entity_refs == frozenset(
        {"ENT_SECURITY_BETA", "ENT_SECURITY_GAMMA"}
    )


def test_scope_manifest_rejects_alias_targets() -> None:
    with pytest.raises(ScopeManifestError) as error:
        holdings_scope_from_manifest({"target_entities": ["security-alpha"]})

    assert error.value.reason == "holdings_scope_manifest_invalid_ref"


def test_producer_filters_to_manifest_target_and_two_hop_context_scope() -> None:
    producer = HoldingsProducer(
        reader=FakeHoldingsMartReader(
            co_holdings=(
                _co_row("co-target-context", "security-alpha", "security-beta"),
                _co_row("co-context-only", "security-beta", "security-gamma"),
                _co_row("co-outside", "security-alpha", "security-delta"),
            ),
            northbound_rows=(
                _northbound_row("nb-target", "security-alpha"),
                _northbound_row("nb-context", "security-gamma"),
                _northbound_row("nb-outside", "security-delta"),
            ),
        ),
        aligner=EntityAligner(
            EntityAlignmentTable(
                holder_nodes={"northbound-holder": "ENT_NORTHBOUND_HOLDER"},
                security_nodes={
                    "security-alpha": "ENT_SECURITY_ALPHA",
                    "security-beta": "ENT_SECURITY_BETA",
                    "security-gamma": "ENT_SECURITY_GAMMA",
                    "security-delta": "ENT_SECURITY_DELTA",
                },
            )
        ),
        scope=HoldingsScope(
            target_entity_refs=frozenset({"ENT_SECURITY_ALPHA"}),
            two_hop_context_entity_refs=frozenset(
                {"ENT_SECURITY_BETA", "ENT_SECURITY_GAMMA"}
            ),
        ),
    )

    result = producer.build_payloads(
        produced_at=datetime(2026, 3, 31, 8, 0, tzinfo=UTC)
    )

    assert [payload["delta_id"] for payload in result.payloads] == [
        "holdings-co-co-target-context",
        "holdings-co-co-context-only",
        "holdings-nb-nb-target",
        "holdings-nb-nb-context",
    ]
    assert {payload["relation_type"] for payload in result.payloads} == {
        "CO_HOLDING",
        "NORTHBOUND_HOLD",
    }
    scope_contexts = [
        payload["producer_context"]["holdings_scope"] for payload in result.payloads
    ]
    assert [context["decision_usage"] for context in scope_contexts] == [
        "decision_target",
        "graph_risk_context",
        "decision_target",
        "graph_risk_context",
    ]
    assert scope_contexts[1]["manifest_target_matched"] is False
    assert scope_contexts[1]["two_hop_context_matched"] is True
    assert [record.reason for record in result.audit] == [
        "scope_filtered",
        "scope_filtered",
    ]
    assert [record.detail["scope_reason"] for record in result.audit] == [
        "company_endpoint_outside_scope",
        "target_outside_scope",
    ]
    for payload in result.payloads:
        wire_payload = {
            key: value
            for key, value in payload.items()
            if key not in {"ex_type", "produced_at"}
        }
        Ex3CandidateGraphDelta.model_validate(wire_payload)


def _lineage() -> LineageSummary:
    return LineageSummary(
        dataset="holdings_derivation_mart",
        snapshot_id="snapshot-scope",
        as_of_date="2026-03-31",
        source_mart="mart_fact_holding_position_v2",
    )


def _co_row(
    row_id: str,
    security_id_left: str,
    security_id_right: str,
) -> FundCoHoldingRow:
    return FundCoHoldingRow(
        row_id=row_id,
        report_date="2026-03-31",
        security_id_left=security_id_left,
        security_id_right=security_id_right,
        co_holding_fund_count=3,
        security_left_fund_count=10,
        security_right_fund_count=8,
        jaccard_score=0.3,
        latest_announced_date="2026-04-30",
        evidence_ref=f"evidence-{row_id}",
        lineage=_lineage(),
    )


def _northbound_row(row_id: str, security_id: str) -> NorthboundZScoreRow:
    return NorthboundZScoreRow(
        row_id=row_id,
        security_id=security_id,
        holder_id="northbound-holder",
        report_date="2026-03-31",
        z_score_metric="holding_ratio",
        lookback_observations=8,
        window_start_date="2025-12-31",
        window_end_date="2026-03-31",
        observation_count=63,
        metric_value=0.018,
        metric_mean=0.011,
        metric_stddev=0.0029,
        metric_z_score=2.4,
        evidence_ref=f"evidence-{row_id}",
        lineage=_lineage(),
    )
