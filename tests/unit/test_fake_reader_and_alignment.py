from subsystem_holdings.alignment import EntityAligner, EntityAlignmentTable
from subsystem_holdings.models import AuditRecord, FundCoHoldingRow, LineageSummary
from subsystem_holdings.producer import HoldingsProducer
from subsystem_holdings.reader import FakeHoldingsMartReader, build_default_fake_reader


def test_fake_reader_returns_expected_mart_shapes() -> None:
    reader = build_default_fake_reader()

    assert reader.canonical_positions()[0].position_id == "position-alpha"
    assert reader.top_holder_qoq_changes()[0].change_id == "top-holder-alpha"
    assert reader.fund_co_holdings()[0].row_id == "coholding-alpha"
    assert reader.northbound_z_scores()[0].row_id == "northbound-alpha"


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
                source_fund_id="fund-missing",
                target_fund_id="fund-beta",
                security_id="security-alpha",
                report_date="2026-03-31",
                co_holding_score=0.7,
                evidence_ref="evidence-unresolved",
                lineage=lineage,
            ),
        )
    )
    aligner = EntityAligner(
        EntityAlignmentTable(
            holder_nodes={"fund-beta": "ENT_FUND_BETA"},
            security_nodes={"security-alpha": "ENT_SECURITY_ALPHA"},
        )
    )

    result = HoldingsProducer(reader, aligner).build_payloads()

    assert result.payloads == ()
    assert result.audit == (
        AuditRecord(
            row_id="coholding-unresolved",
            reason="unresolved_holder",
            detail="fund-missing",
        ),
    )
