from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Mapping

from subsystem_holdings.alignment import EntityAlignmentResolver
from subsystem_holdings.errors import PayloadValidationError
from subsystem_holdings.models import (
    AuditRecord,
    Ex3Payload,
    FundCoHoldingRow,
    NorthboundZScoreRow,
    ProducerResult,
)
from subsystem_holdings.reader import HoldingsMartReader
from subsystem_holdings.scope import HoldingsScope, ScopeCheck

SUBSYSTEM_ID = "subsystem-holdings"
FUND_CO_HOLDING_DERIVATION_MART = "mart_deriv_fund_co_holding"
NORTHBOUND_Z_SCORE_DERIVATION_MART = "mart_deriv_northbound_holding_z_score"
TOP_HOLDER_QOQ_DERIVATION_MART = "mart_deriv_top_holder_qoq_change"


def _wire_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in payload.items()
        if key not in {"ex_type", "produced_at"}
    }


def _validate_ex3_wire_payload(payload: Mapping[str, Any]) -> None:
    try:
        from contracts.schemas import Ex3CandidateGraphDelta

        Ex3CandidateGraphDelta.model_validate(_wire_payload(payload))
    except Exception as exc:  # pragma: no cover - preserves upstream details.
        raise PayloadValidationError(str(exc)) from exc


@dataclass(slots=True)
class HoldingsProducer:
    reader: HoldingsMartReader
    aligner: EntityAlignmentResolver
    subsystem_id: str = SUBSYSTEM_ID
    scope: HoldingsScope | None = None

    def build_payloads(self, *, produced_at: datetime | None = None) -> ProducerResult:
        timestamp = produced_at or datetime.now(UTC)
        payloads: list[Ex3Payload] = []
        audit: list[AuditRecord] = []

        for row in self.reader.fund_co_holdings():
            payload = self._co_holding_payload(row, timestamp, audit)
            if payload is not None:
                payloads.append(payload)

        for row in self.reader.northbound_z_scores():
            payload = self._northbound_payload(row, timestamp, audit)
            if payload is not None:
                payloads.append(payload)

        for row in self.reader.top_holder_qoq_changes():
            audit.append(
                AuditRecord(
                    row_id=row.row_id,
                    reason="read_only_input",
                    detail={
                        "message": (
                            "top-holder quarter-over-quarter input is not submitted "
                            "in PR1"
                        ),
                        "source_mart": TOP_HOLDER_QOQ_DERIVATION_MART,
                        "mart_row": row.as_mart_properties(),
                        "lineage": row.lineage.as_properties(),
                    },
                )
            )

        for payload in payloads:
            _validate_ex3_wire_payload(payload)

        return ProducerResult(
            payloads=tuple(payloads),
            audit=tuple(audit),
            produced_at=timestamp,
        )

    def submit(
        self,
        submit_client: Any,
        *,
        produced_at: datetime | None = None,
    ) -> ProducerResult:
        result = self.build_payloads(produced_at=produced_at)
        receipts = tuple(submit_client.submit(payload) for payload in result.payloads)
        return ProducerResult(
            payloads=result.payloads,
            audit=result.audit,
            receipts=receipts,
            produced_at=result.produced_at,
        )

    def _co_holding_payload(
        self,
        row: FundCoHoldingRow,
        produced_at: datetime,
        audit: list[AuditRecord],
    ) -> Ex3Payload | None:
        source = self.aligner.security(row.security_id_left)
        target = self.aligner.security(row.security_id_right)
        if not source.resolved:
            _append_unresolved_alignment_audit(
                audit, row.row_id, "unresolved_security", source
            )
            return None
        if not target.resolved:
            _append_unresolved_alignment_audit(
                audit, row.row_id, "unresolved_security", target
            )
            return None
        scope_check = self._scope_check(
            relation_type="CO_HOLDING",
            source_node=source.node_id,
            target_node=target.node_id,
        )
        if scope_check is not None and not scope_check.allowed:
            _append_scope_filtered_audit(audit, row.row_id, "CO_HOLDING", scope_check)
            return None

        payload: Ex3Payload = {
            "ex_type": "Ex-3",
            "subsystem_id": self.subsystem_id,
            "produced_at": produced_at.isoformat().replace("+00:00", "Z"),
            "delta_id": f"holdings-co-{row.row_id}",
            "delta_type": "edge_upsert",
            "source_node": source.node_id,
            "target_node": target.node_id,
            "relation_type": "CO_HOLDING",
            "properties": {
                "report_date": row.report_date,
                "co_holding_fund_count": row.co_holding_fund_count,
                "security_left_fund_count": row.security_left_fund_count,
                "security_right_fund_count": row.security_right_fund_count,
                "jaccard_score": row.jaccard_score,
                "latest_announced_date": row.latest_announced_date,
                "lineage": row.lineage.as_properties(),
            },
            "evidence": [
                row.evidence_ref,
                (
                    f"{FUND_CO_HOLDING_DERIVATION_MART}:"
                    f"co_holding_fund_count={row.co_holding_fund_count};"
                    f"jaccard_score={row.jaccard_score};"
                    f"latest_announced_date={row.latest_announced_date}"
                ),
                row.lineage.as_evidence_summary(),
            ],
            "producer_context": {
                "mart_row_id": row.row_id,
                "source_mart": FUND_CO_HOLDING_DERIVATION_MART,
                "source_shape": "mart_deriv_fund_co_holding_security_pair",
                "lineage": row.lineage.as_properties(),
            },
        }
        _append_scope_context(payload, scope_check)
        return payload

    def _northbound_payload(
        self,
        row: NorthboundZScoreRow,
        produced_at: datetime,
        audit: list[AuditRecord],
    ) -> Ex3Payload | None:
        holder = self.aligner.holder(row.holder_id)
        security = self.aligner.security(row.security_id)
        if not holder.resolved:
            _append_unresolved_alignment_audit(
                audit, row.row_id, "unresolved_holder", holder
            )
            return None
        if not security.resolved:
            _append_unresolved_alignment_audit(
                audit, row.row_id, "unresolved_security", security
            )
            return None
        scope_check = self._scope_check(
            relation_type="NORTHBOUND_HOLD",
            source_node=holder.node_id,
            target_node=security.node_id,
        )
        if scope_check is not None and not scope_check.allowed:
            _append_scope_filtered_audit(
                audit, row.row_id, "NORTHBOUND_HOLD", scope_check
            )
            return None

        payload: Ex3Payload = {
            "ex_type": "Ex-3",
            "subsystem_id": self.subsystem_id,
            "produced_at": produced_at.isoformat().replace("+00:00", "Z"),
            "delta_id": f"holdings-nb-{row.row_id}",
            "delta_type": "edge_upsert",
            "source_node": holder.node_id,
            "target_node": security.node_id,
            "relation_type": "NORTHBOUND_HOLD",
            "properties": {
                "report_date": row.report_date,
                "z_score_metric": row.z_score_metric,
                "lookback_observations": row.lookback_observations,
                "window_start_date": row.window_start_date,
                "window_end_date": row.window_end_date,
                "observation_count": row.observation_count,
                "metric_value": row.metric_value,
                "metric_mean": row.metric_mean,
                "metric_stddev": row.metric_stddev,
                "metric_z_score": row.metric_z_score,
                "lineage": row.lineage.as_properties(),
            },
            "evidence": [
                row.evidence_ref,
                (
                    f"{NORTHBOUND_Z_SCORE_DERIVATION_MART}:"
                    f"z_score_metric={row.z_score_metric};"
                    f"lookback_observations={row.lookback_observations};"
                    f"window_start_date={row.window_start_date};"
                    f"window_end_date={row.window_end_date};"
                    f"observation_count={row.observation_count};"
                    f"metric_value={row.metric_value};"
                    f"metric_mean={row.metric_mean};"
                    f"metric_stddev={row.metric_stddev};"
                    f"metric_z_score={row.metric_z_score}"
                ),
                row.lineage.as_evidence_summary(),
            ],
            "producer_context": {
                "mart_row_id": row.row_id,
                "source_mart": NORTHBOUND_Z_SCORE_DERIVATION_MART,
                "source_shape": "mart_deriv_northbound_holding_z_score",
                "lineage": row.lineage.as_properties(),
            },
        }
        _append_scope_context(payload, scope_check)
        return payload

    def _scope_check(
        self,
        *,
        relation_type: str,
        source_node: str | None,
        target_node: str | None,
    ) -> ScopeCheck | None:
        if self.scope is None:
            return None
        if source_node is None or target_node is None:
            return ScopeCheck(
                allowed=False,
                reason="missing_endpoint",
                usage=None,
                source_role="outside_scope",
                target_role="outside_scope",
            )
        return self.scope.evaluate(
            relation_type=relation_type,
            source_node=source_node,
            target_node=target_node,
        )


def _append_unresolved_alignment_audit(
    audit: list[AuditRecord],
    row_id: str,
    reason: str,
    decision: Any,
) -> None:
    if (
        getattr(decision, "reason", None) is None
        and getattr(decision, "metadata", None) is None
    ):
        audit.append(AuditRecord(row_id, reason, decision.source_id))
        return

    detail: dict[str, object] = {"source_id": decision.source_id}
    if decision.reason is not None:
        detail["alignment_reason"] = decision.reason
    if decision.metadata is not None:
        detail["alignment_metadata"] = dict(decision.metadata)
    audit.append(AuditRecord(row_id, reason, detail))


def _append_scope_filtered_audit(
    audit: list[AuditRecord],
    row_id: str,
    relation_type: str,
    scope_check: ScopeCheck,
) -> None:
    audit.append(
        AuditRecord(
            row_id=row_id,
            reason="scope_filtered",
            detail={
                "relation_type": relation_type,
                "scope_reason": scope_check.reason,
                "source_role": scope_check.source_role,
                "target_role": scope_check.target_role,
            },
        )
    )


def _append_scope_context(
    payload: Ex3Payload,
    scope_check: ScopeCheck | None,
) -> None:
    if scope_check is None:
        return
    payload["producer_context"]["holdings_scope"] = scope_check.as_context()
