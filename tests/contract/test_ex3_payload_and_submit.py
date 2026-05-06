from datetime import UTC, datetime

from contracts.schemas import Ex3CandidateGraphDelta
from subsystem_sdk.backends import MockSubmitBackend
from subsystem_sdk.submit import SubmitClient

from subsystem_holdings.public import build_default_offline_producer


def test_ex3_payloads_validate_against_contract() -> None:
    producer = build_default_offline_producer()
    result = producer.build_payloads(
        produced_at=datetime(2026, 3, 31, 8, 0, tzinfo=UTC)
    )

    assert len(result.payloads) == 2
    for payload in result.payloads:
        wire_payload = {
            key: value
            for key, value in payload.items()
            if key not in {"ex_type", "produced_at"}
        }
        Ex3CandidateGraphDelta.model_validate(wire_payload)


def test_payloads_keep_holdings_mart_shapes() -> None:
    producer = build_default_offline_producer()
    result = producer.build_payloads(
        produced_at=datetime(2026, 3, 31, 8, 0, tzinfo=UTC)
    )
    payloads = {payload["relation_type"]: payload for payload in result.payloads}
    co_holding_lineage = {
        "dataset": "holdings_derivation_mart",
        "snapshot_id": "snapshot-alpha",
        "as_of_date": "2026-03-31",
        "source_mart": "mart_fact_holding_position_v2",
        "source_window_start_date": "2026-03-31",
        "source_window_end_date": "2026-03-31",
        "source_row_count": 54,
        "source_lineage_row_count": 12,
        "source_lineage_summary": (
            "fund-position rows aggregated into security-pair overlap"
        ),
        "raw_loaded_at_min": "2026-04-30T00:00:00Z",
        "raw_loaded_at_max": "2026-04-30T00:05:00Z",
        "source_interface_ids": ["fund-position-summary"],
        "source_run_ids": ["holdings-derivation-run-alpha"],
    }
    northbound_lineage = {
        "dataset": "holdings_derivation_mart",
        "snapshot_id": "snapshot-alpha",
        "as_of_date": "2026-03-31",
        "source_mart": "mart_fact_holding_position_v2",
        "source_window_start_date": "2025-12-31",
        "source_window_end_date": "2026-03-31",
        "source_row_count": 8,
        "source_lineage_row_count": 8,
        "source_lineage_summary": (
            "windowed holding-ratio observations standardized per holder-security"
        ),
        "raw_loaded_at_min": "2026-04-30T00:00:00Z",
        "raw_loaded_at_max": "2026-04-30T00:05:00Z",
        "source_interface_ids": ["northbound-position-summary"],
        "source_run_ids": ["holdings-derivation-run-alpha"],
    }

    co_holding = payloads["CO_HOLDING"]
    assert co_holding["source_node"] == "ENT_SECURITY_ALPHA"
    assert co_holding["target_node"] == "ENT_SECURITY_BETA"
    assert co_holding["properties"] == {
        "report_date": "2026-03-31",
        "co_holding_fund_count": 12,
        "security_left_fund_count": 30,
        "security_right_fund_count": 24,
        "jaccard_score": 0.286,
        "latest_announced_date": "2026-04-30",
        "lineage": co_holding_lineage,
    }
    assert "co_holding_fund_count=12" in co_holding["evidence"][1]
    assert "jaccard_score=0.286" in co_holding["evidence"][1]
    assert "latest_announced_date=2026-04-30" in co_holding["evidence"][1]
    assert "source_mart=mart_fact_holding_position_v2" in co_holding["evidence"][2]
    assert co_holding["producer_context"]["source_mart"] == (
        "mart_deriv_fund_co_holding"
    )
    assert co_holding["producer_context"]["lineage"] == co_holding_lineage

    northbound = payloads["NORTHBOUND_HOLD"]
    assert northbound["source_node"] == "ENT_NORTHBOUND_HOLDER"
    assert northbound["target_node"] == "ENT_SECURITY_ALPHA"
    assert northbound["properties"] == {
        "report_date": "2026-03-31",
        "z_score_metric": "holding_ratio",
        "lookback_observations": 8,
        "window_start_date": "2025-12-31",
        "window_end_date": "2026-03-31",
        "observation_count": 63,
        "metric_value": 0.018,
        "metric_mean": 0.011,
        "metric_stddev": 0.0029,
        "metric_z_score": 2.4,
        "lineage": northbound_lineage,
    }
    assert "trade_date" not in northbound["properties"]
    assert "z_score" not in northbound["properties"]
    assert "holding_ratio" not in northbound["properties"]
    assert "lookback_window_days" not in northbound["properties"]
    assert (
        northbound["producer_context"]["source_shape"]
        == "mart_deriv_northbound_holding_z_score"
    )
    assert (
        northbound["producer_context"]["source_mart"]
        == "mart_deriv_northbound_holding_z_score"
    )
    assert northbound["evidence"][1].startswith(
        "mart_deriv_northbound_holding_z_score:"
    )
    assert "z_score_metric=holding_ratio" in northbound["evidence"][1]
    assert "lookback_observations=8" in northbound["evidence"][1]
    assert "window_start_date=2025-12-31" in northbound["evidence"][1]
    assert "window_end_date=2026-03-31" in northbound["evidence"][1]
    assert "observation_count=63" in northbound["evidence"][1]
    assert "metric_value=0.018" in northbound["evidence"][1]
    assert "metric_mean=0.011" in northbound["evidence"][1]
    assert "metric_stddev=0.0029" in northbound["evidence"][1]
    assert "metric_z_score=2.4" in northbound["evidence"][1]
    assert (
        "source_mart=mart_fact_holding_position_v2"
        in northbound["evidence"][2]
    )
    assert northbound["producer_context"]["lineage"] == northbound_lineage


def test_mock_submit_backend_receives_valid_wire_payloads() -> None:
    backend = MockSubmitBackend()
    client = SubmitClient(backend)
    producer = build_default_offline_producer()

    result = producer.submit(
        client,
        produced_at=datetime(2026, 3, 31, 8, 0, tzinfo=UTC),
    )

    assert len(result.receipts) == 2
    assert len(backend.submitted_payloads) == 2
    relation_types = {payload["relation_type"] for payload in backend.submitted_payloads}
    assert relation_types == {"CO_HOLDING", "NORTHBOUND_HOLD"}
    for payload in backend.submitted_payloads:
        assert "ex_type" not in payload
        assert "produced_at" not in payload
        Ex3CandidateGraphDelta.model_validate(payload)
