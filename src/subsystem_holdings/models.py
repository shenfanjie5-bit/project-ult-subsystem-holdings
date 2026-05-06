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

    def as_properties(self) -> dict[str, object]:
        return {
            "dataset": self.dataset,
            "snapshot_id": self.snapshot_id,
            "as_of_date": self.as_of_date,
        }


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
    change_id: str
    holder_id: str
    security_id: str
    report_date: str
    ratio_delta: float
    lineage: LineageSummary


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
    lookback_window_days: int
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
    detail: str


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
