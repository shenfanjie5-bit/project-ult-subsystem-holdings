from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Protocol

from subsystem_holdings.models import (
    CanonicalHoldingPosition,
    FundCoHoldingRow,
    LineageSummary,
    NorthboundZScoreRow,
    TopHolderQoQChange,
)


class HoldingsMartReader(Protocol):
    def canonical_positions(self) -> Sequence[CanonicalHoldingPosition]: ...

    def top_holder_qoq_changes(self) -> Sequence[TopHolderQoQChange]: ...

    def fund_co_holdings(self) -> Sequence[FundCoHoldingRow]: ...

    def northbound_z_scores(self) -> Sequence[NorthboundZScoreRow]: ...


@dataclass(frozen=True, slots=True)
class FakeHoldingsMartReader:
    positions: tuple[CanonicalHoldingPosition, ...] = field(default_factory=tuple)
    top_holder_changes: tuple[TopHolderQoQChange, ...] = field(default_factory=tuple)
    co_holdings: tuple[FundCoHoldingRow, ...] = field(default_factory=tuple)
    northbound_rows: tuple[NorthboundZScoreRow, ...] = field(default_factory=tuple)

    def canonical_positions(self) -> Sequence[CanonicalHoldingPosition]:
        return self.positions

    def top_holder_qoq_changes(self) -> Sequence[TopHolderQoQChange]:
        return self.top_holder_changes

    def fund_co_holdings(self) -> Sequence[FundCoHoldingRow]:
        return self.co_holdings

    def northbound_z_scores(self) -> Sequence[NorthboundZScoreRow]:
        return self.northbound_rows


def build_default_fake_reader() -> FakeHoldingsMartReader:
    lineage = LineageSummary(
        dataset="holdings_canonical_mart",
        snapshot_id="snapshot-alpha",
        as_of_date="2026-03-31",
    )
    return FakeHoldingsMartReader(
        positions=(
            CanonicalHoldingPosition(
                position_id="position-alpha",
                holder_id="fund-alpha",
                security_id="security-alpha",
                report_date="2026-03-31",
                holding_ratio=0.042,
                lineage=lineage,
            ),
        ),
        top_holder_changes=(
            TopHolderQoQChange(
                change_id="top-holder-alpha",
                holder_id="holder-alpha",
                security_id="security-alpha",
                report_date="2026-03-31",
                ratio_delta=0.011,
                lineage=lineage,
            ),
        ),
        co_holdings=(
            FundCoHoldingRow(
                row_id="coholding-alpha",
                report_date="2026-03-31",
                security_id_left="security-alpha",
                security_id_right="security-beta",
                co_holding_fund_count=12,
                security_left_fund_count=30,
                security_right_fund_count=24,
                jaccard_score=0.286,
                latest_announced_date="2026-04-30",
                evidence_ref="evidence-coholding-alpha",
                lineage=lineage,
            ),
        ),
        northbound_rows=(
            NorthboundZScoreRow(
                row_id="northbound-alpha",
                security_id="security-alpha",
                holder_id="northbound-holder",
                report_date="2026-03-31",
                z_score_metric="holding_ratio",
                lookback_observations=90,
                window_start_date="2025-12-31",
                window_end_date="2026-03-31",
                observation_count=63,
                metric_value=0.018,
                metric_mean=0.011,
                metric_stddev=0.0029,
                metric_z_score=2.4,
                evidence_ref="evidence-northbound-alpha",
                lineage=lineage,
            ),
        ),
    )
