from subsystem_holdings.alignment import EntityAligner, EntityAlignmentTable
from subsystem_holdings.models import AuditRecord, FundCoHoldingRow, LineageSummary
from subsystem_holdings.producer import HoldingsProducer
from subsystem_holdings.reader import FakeHoldingsMartReader, build_default_fake_reader


def test_fake_reader_returns_expected_mart_shapes() -> None:
    reader = build_default_fake_reader()

    assert reader.canonical_positions()[0].position_id == "position-alpha"
    assert reader.top_holder_qoq_changes()[0].change_id == "top-holder-alpha"
    co_holding = reader.fund_co_holdings()[0]
    assert co_holding.row_id == "coholding-alpha"
    assert co_holding.security_id_left == "security-alpha"
    assert co_holding.security_id_right == "security-beta"
    assert co_holding.co_holding_fund_count == 12
    assert co_holding.jaccard_score == 0.286
    northbound = reader.northbound_z_scores()[0]
    assert northbound.row_id == "northbound-alpha"
    assert northbound.report_date == "2026-03-31"
    assert northbound.z_score_metric == "holding_ratio"
    assert northbound.lookback_observations == 90
    assert northbound.window_start_date == "2025-12-31"
    assert northbound.window_end_date == "2026-03-31"
    assert northbound.metric_z_score == 2.4


def test_unresolved_alignment_fails_closed_to_audit() -> None:
    lineage = LineageSummary(
        dataset="holdings_canonical_mart",
        snapshot_id="snapshot-beta",
        as_of_date="2026-03-31",
    )
    reader = FakeHoldingsMartReader(
        co_holdings=(
            FundCoHoldingRow(
                row_id="coholding-unresolved",
                report_date="2026-03-31",
                security_id_left="security-missing",
                security_id_right="security-beta",
                co_holding_fund_count=8,
                security_left_fund_count=19,
                security_right_fund_count=21,
                jaccard_score=0.25,
                latest_announced_date="2026-04-30",
                evidence_ref="evidence-unresolved",
                lineage=lineage,
            ),
        )
    )
    aligner = EntityAligner(
        EntityAlignmentTable(
            security_nodes={"security-beta": "ENT_SECURITY_BETA"},
        )
    )

    result = HoldingsProducer(reader, aligner).build_payloads()

    assert result.payloads == ()
    assert result.audit == (
        AuditRecord(
            row_id="coholding-unresolved",
            reason="unresolved_security",
            detail="security-missing",
        ),
    )
