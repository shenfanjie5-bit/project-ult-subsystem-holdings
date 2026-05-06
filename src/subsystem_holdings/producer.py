from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Mapping

from subsystem_holdings.alignment import EntityAligner
from subsystem_holdings.errors import PayloadValidationError
from subsystem_holdings.models import (
    AuditRecord,
    Ex3Payload,
    FundCoHoldingRow,
    NorthboundZScoreRow,
    ProducerResult,
)
from subsystem_holdings.reader import HoldingsMartReader

SUBSYSTEM_ID = "subsystem-holdings"


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
    aligner: EntityAligner
    subsystem_id: str = SUBSYSTEM_ID

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
                    row_id=row.change_id,
                    reason="read_only_input",
                    detail="top-holder quarter-over-quarter input is not submitted in PR1",
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
            audit.append(
                AuditRecord(row.row_id, "unresolved_security", row.security_id_left)
            )
            return None
        if not target.resolved:
            audit.append(
                AuditRecord(row.row_id, "unresolved_security", row.security_id_right)
            )
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
                    "mart_deriv_fund_co_holding:"
                    f"co_holding_fund_count={row.co_holding_fund_count};"
                    f"jaccard_score={row.jaccard_score};"
                    f"latest_announced_date={row.latest_announced_date}"
                ),
            ],
            "producer_context": {
                "mart_row_id": row.row_id,
                "source_shape": "mart_deriv_fund_co_holding_security_pair",
            },
        }
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
            audit.append(AuditRecord(row.row_id, "unresolved_holder", row.holder_id))
            return None
        if not security.resolved:
            audit.append(AuditRecord(row.row_id, "unresolved_security", row.security_id))
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
                "lookback_window_days": row.lookback_window_days,
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
                    "mart_deriv_northbound_z_score:"
                    f"z_score_metric={row.z_score_metric};"
                    f"metric_z_score={row.metric_z_score}"
                ),
            ],
            "producer_context": {
                "mart_row_id": row.row_id,
                "source_shape": "mart_deriv_northbound_z_score",
            },
        }
        return payload
