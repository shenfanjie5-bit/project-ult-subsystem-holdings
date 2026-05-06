from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal, TypeAlias

RelationType: TypeAlias = Literal["CO_HOLDING", "NORTHBOUND_HOLD"]
AuditReason: TypeAlias = Literal["unresolved_holder", "unresolved_security", "read_only_input"]


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
    source_fund_id: str
    target_fund_id: str
    security_id: str
    report_date: str
    co_holding_score: float
    evidence_ref: str
    lineage: LineageSummary


@dataclass(frozen=True, slots=True)
class NorthboundZScoreRow:
    row_id: str
    holder_id: str
    security_id: str
    trade_date: str
    z_score: float
    holding_ratio: float
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
