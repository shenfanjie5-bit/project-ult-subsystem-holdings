from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal, TypeAlias

RelationType: TypeAlias = Literal["CO_HOLDING", "NORTHBOUND_HOLD"]
AuditReason: TypeAlias = Literal["unresolved_holder", "unresolved_security", "read_only_input"]
ZScoreMetric: TypeAlias = Literal["holding_amount", "holding_ratio"]


@dataclass(frozen=True, slots=True)
class LineageSummary:
    dataset: str
    snapshot_id: str
    as_of_date: str
    source_mart: str | None = None
    source_window_start_date: str | None = None
    source_window_end_date: str | None = None
    source_interface_ids_summary: tuple[str, ...] = ()
    source_row_count: int | None = None
    lineage_row_count: int | None = None
    lineage_summary: str | None = None
    source_run_ids_summary: tuple[str, ...] = ()
    source_load_started_at: str | None = None
    source_load_finished_at: str | None = None

    def as_properties(self) -> dict[str, object]:
        properties: dict[str, object] = {
            "dataset": self.dataset,
            "snapshot_id": self.snapshot_id,
            "as_of_date": self.as_of_date,
        }
        optional: dict[str, object | None] = {
            "source_mart": self.source_mart,
            "source_window_start_date": self.source_window_start_date,
            "source_window_end_date": self.source_window_end_date,
            "source_row_count": self.source_row_count,
            "lineage_row_count": self.lineage_row_count,
            "lineage_summary": self.lineage_summary,
            "source_load_started_at": self.source_load_started_at,
            "source_load_finished_at": self.source_load_finished_at,
        }
        properties.update(
            {key: value for key, value in optional.items() if value is not None}
        )
        if self.source_interface_ids_summary:
            properties["source_interface_ids_summary"] = list(
                self.source_interface_ids_summary
            )
        if self.source_run_ids_summary:
            properties["source_run_ids_summary"] = list(self.source_run_ids_summary)
        return properties

    def as_evidence_summary(self) -> str:
        parts = [
            f"dataset={self.dataset}",
            f"snapshot_id={self.snapshot_id}",
            f"as_of_date={self.as_of_date}",
        ]
        if self.source_mart is not None:
            parts.append(f"source_mart={self.source_mart}")
        if self.source_window_start_date is not None:
            parts.append(f"source_window_start_date={self.source_window_start_date}")
        if self.source_window_end_date is not None:
            parts.append(f"source_window_end_date={self.source_window_end_date}")
        if self.source_row_count is not None:
            parts.append(f"source_row_count={self.source_row_count}")
        if self.lineage_row_count is not None:
            parts.append(f"lineage_row_count={self.lineage_row_count}")
        if self.lineage_summary is not None:
            parts.append(f"lineage_summary={self.lineage_summary}")
        return "lineage:" + ";".join(parts)


@dataclass(frozen=True, slots=True)
class CanonicalHoldingPosition:
    position_id: str
    holder_id: str
    security_id: str
    report_date: str
    holding_ratio: float
    lineage: LineageSummary


@dataclass(frozen=True, slots=True)
class TopHolderQoQChange:
    holding_source: str
    holder_id: str
    security_id: str
    report_date: str
    announced_date: str
    previous_report_date: str
    previous_announced_date: str
    holding_amount: float
    previous_holding_amount: float
    holding_amount_delta: float
    holding_amount_delta_pct: float
    holding_ratio: float
    previous_holding_ratio: float
    holding_ratio_delta: float
    lineage: LineageSummary

    @property
    def row_id(self) -> str:
        return ":".join(
            (
                self.holding_source,
                self.holder_id,
                self.security_id,
                self.report_date,
            )
        )

    def as_mart_properties(self) -> dict[str, object]:
        return {
            "holding_source": self.holding_source,
            "holder_id": self.holder_id,
            "security_id": self.security_id,
            "report_date": self.report_date,
            "announced_date": self.announced_date,
            "previous_report_date": self.previous_report_date,
            "previous_announced_date": self.previous_announced_date,
            "holding_amount": self.holding_amount,
            "previous_holding_amount": self.previous_holding_amount,
            "holding_amount_delta": self.holding_amount_delta,
            "holding_amount_delta_pct": self.holding_amount_delta_pct,
            "holding_ratio": self.holding_ratio,
            "previous_holding_ratio": self.previous_holding_ratio,
            "holding_ratio_delta": self.holding_ratio_delta,
        }


@dataclass(frozen=True, slots=True)
class FundCoHoldingRow:
    row_id: str
    report_date: str
    security_id_left: str
    security_id_right: str
    co_holding_fund_count: int
    security_left_fund_count: int
    security_right_fund_count: int
    jaccard_score: float
    latest_announced_date: str
    evidence_ref: str
    lineage: LineageSummary


@dataclass(frozen=True, slots=True)
class NorthboundZScoreRow:
    row_id: str
    security_id: str
    holder_id: str
    report_date: str
    z_score_metric: ZScoreMetric
    lookback_observations: int
    window_start_date: str
    window_end_date: str
    observation_count: int
    metric_value: float
    metric_mean: float
    metric_stddev: float
    metric_z_score: float
    evidence_ref: str
    lineage: LineageSummary


@dataclass(frozen=True, slots=True)
class AuditRecord:
    row_id: str
    reason: AuditReason
    detail: object


@dataclass(frozen=True, slots=True)
class AlignmentDecision:
    source_id: str
    node_id: str | None

    @property
    def resolved(self) -> bool:
        return self.node_id is not None


Ex3Payload: TypeAlias = dict[str, Any]


@dataclass(frozen=True, slots=True)
class ProducerResult:
    payloads: tuple[Ex3Payload, ...]
    audit: tuple[AuditRecord, ...]
    receipts: tuple[Any, ...] = field(default_factory=tuple)
    produced_at: datetime | None = None
