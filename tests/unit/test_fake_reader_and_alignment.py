from subsystem_holdings.alignment import EntityAligner, EntityAlignmentTable
from subsystem_holdings.models import AuditRecord, FundCoHoldingRow, LineageSummary
from subsystem_holdings.producer import HoldingsProducer
from subsystem_holdings.reader import FakeHoldingsMartReader, build_default_fake_reader


def test_fake_reader_returns_expected_mart_shapes() -> None:
    reader = build_default_fake_reader()

    assert reader.canonical_positions()[0].position_id == "position-alpha"
    top_holder = reader.top_holder_qoq_changes()[0]
    assert top_holder.as_mart_properties() == {
        "holding_source": "quarterly_report",
        "holder_id": "holder-alpha",
        "security_id": "security-alpha",
        "report_date": "2026-03-31",
        "announced_date": "2026-04-30",
        "previous_report_date": "2025-12-31",
        "previous_announced_date": "2026-01-31",
        "holding_amount": 4200000.0,
        "previous_holding_amount": 3900000.0,
        "holding_amount_delta": 300000.0,
        "holding_amount_delta_pct": 0.0769,
        "holding_ratio": 0.042,
        "previous_holding_ratio": 0.031,
        "holding_ratio_delta": 0.011,
    }
    assert top_holder.lineage.as_properties()["source_mart"] == (
        "mart_deriv_top_holder_qoq_change"
    )
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
    assert northbound.lookback_observations == 8
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


def test_top_holder_qoq_rows_stay_read_only_with_mart_shape_audit() -> None:
    reader = build_default_fake_reader()
    aligner = EntityAligner(
        EntityAlignmentTable(
            holder_nodes={"holder-alpha": "ENT_HOLDER_ALPHA"},
            security_nodes={"security-alpha": "ENT_SECURITY_ALPHA"},
        )
    )

    result = HoldingsProducer(
        FakeHoldingsMartReader(top_holder_changes=reader.top_holder_qoq_changes()),
        aligner,
    ).build_payloads()

    assert result.payloads == ()
    assert len(result.audit) == 1
    audit = result.audit[0]
    assert audit.row_id == "quarterly_report:holder-alpha:security-alpha:2026-03-31"
    assert audit.reason == "read_only_input"
    assert isinstance(audit.detail, dict)
    assert audit.detail["source_mart"] == "mart_deriv_top_holder_qoq_change"
    top_holder_row = reader.top_holder_qoq_changes()[0]
    assert audit.detail["mart_row"] == top_holder_row.as_mart_properties()
    assert audit.detail["lineage"]["source_row_count"] == 2
