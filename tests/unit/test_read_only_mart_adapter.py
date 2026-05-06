from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pytest

from subsystem_holdings.alignment import EntityAligner, EntityAlignmentTable
from subsystem_holdings.errors import AdapterSchemaError
from subsystem_holdings.mart_adapter import (
    FUND_CO_HOLDING_LINEAGE_MART,
    FUND_CO_HOLDING_MART,
    NORTHBOUND_Z_SCORE_LINEAGE_MART,
    NORTHBOUND_Z_SCORE_MART,
    TOP_HOLDER_QOQ_LINEAGE_MART,
    TOP_HOLDER_QOQ_MART,
    ReadOnlyMartAdapter,
)
from subsystem_holdings.producer import HoldingsProducer


CO_REQUIRED = (
    "mart_key",
    "row_id",
    "report_date",
    "security_id_left",
    "security_id_right",
    "co_holding_fund_count",
    "security_left_fund_count",
    "security_right_fund_count",
    "jaccard_score",
    "latest_announced_date",
    "evidence_ref",
)

def test_duckdb_adapter_normal_read_feeds_producer(tmp_path: Path) -> None:
    db_path = tmp_path / "holdings.duckdb"
    connection = duckdb.connect(str(db_path))
    try:
        _create_all_tables(connection)
        _insert_normal_rows(connection)
    finally:
        connection.close()

    adapter = ReadOnlyMartAdapter.from_duckdb_path(db_path)
    producer = HoldingsProducer(
        adapter,
        EntityAligner(
            EntityAlignmentTable(
                holder_nodes={"northbound-holder": "ENT_NORTHBOUND_HOLDER"},
                security_nodes={
                    "security-alpha": "ENT_SECURITY_ALPHA",
                    "security-beta": "ENT_SECURITY_BETA",
                },
            )
        ),
    )

    result = producer.build_payloads(
        produced_at=datetime(2026, 3, 31, 8, 0, tzinfo=UTC)
    )

    assert [payload["relation_type"] for payload in result.payloads] == [
        "CO_HOLDING",
        "NORTHBOUND_HOLD",
    ]
    assert len(result.audit) == 1
    assert result.audit[0].reason == "read_only_input"
    assert result.audit[0].detail["source_mart"] == TOP_HOLDER_QOQ_MART
    assert result.payloads[0]["producer_context"]["lineage"]["source_run_ids"] == [
        "run-co"
    ]
    assert result.payloads[1]["producer_context"]["lineage"]["source_run_ids"] == [
        "run-nb"
    ]
    assert adapter.diagnostics == []


def test_missing_table_fails_closed_with_diagnostic(tmp_path: Path) -> None:
    db_path = tmp_path / "empty.duckdb"
    duckdb.connect(str(db_path)).close()
    adapter = ReadOnlyMartAdapter.from_duckdb_path(db_path)

    assert adapter.fund_co_holdings() == ()

    assert len(adapter.diagnostics) == 1
    diagnostic = adapter.diagnostics[0]
    assert diagnostic.table == FUND_CO_HOLDING_MART
    assert diagnostic.reason == "missing_table"
    assert diagnostic.row_key is None


def test_empty_result_returns_empty_sequences_and_zero_payloads(tmp_path: Path) -> None:
    db_path = tmp_path / "empty_rows.duckdb"
    connection = duckdb.connect(str(db_path))
    try:
        _create_all_tables(connection)
    finally:
        connection.close()
    adapter = ReadOnlyMartAdapter.from_duckdb_path(db_path)

    result = HoldingsProducer(
        adapter,
        EntityAligner(EntityAlignmentTable()),
    ).build_payloads()

    assert result.payloads == ()
    assert result.audit == ()
    assert adapter.diagnostics == []


def test_schema_mismatch_raises_adapter_schema_error(tmp_path: Path) -> None:
    db_path = tmp_path / "bad_schema.duckdb"
    connection = duckdb.connect(str(db_path))
    try:
        connection.execute(
            f"""
            CREATE TABLE {FUND_CO_HOLDING_MART} (
                mart_key VARCHAR,
                report_date VARCHAR
            )
            """
        )
    finally:
        connection.close()
    adapter = ReadOnlyMartAdapter.from_duckdb_path(db_path)

    with pytest.raises(AdapterSchemaError, match="missing required columns"):
        adapter.fund_co_holdings()

    assert adapter.diagnostics[0].table == FUND_CO_HOLDING_MART
    assert adapter.diagnostics[0].reason == "schema_mismatch"


def test_missing_lineage_fails_closed_without_payload(tmp_path: Path) -> None:
    db_path = tmp_path / "missing_lineage.duckdb"
    connection = duckdb.connect(str(db_path))
    try:
        _create_fund_co_table(connection)
        _create_lineage_table(connection, FUND_CO_HOLDING_LINEAGE_MART)
        _insert_co_holding(connection)
    finally:
        connection.close()
    adapter = ReadOnlyMartAdapter.from_duckdb_path(db_path)

    rows = adapter.fund_co_holdings()

    assert rows == ()
    assert len(adapter.diagnostics) == 1
    assert adapter.diagnostics[0].table == FUND_CO_HOLDING_MART
    assert adapter.diagnostics[0].reason == "missing_lineage"
    assert adapter.diagnostics[0].row_key == "co-key"

    result = HoldingsProducer(
        adapter,
        EntityAligner(
            EntityAlignmentTable(
                security_nodes={
                    "security-alpha": "ENT_SECURITY_ALPHA",
                    "security-beta": "ENT_SECURITY_BETA",
                }
            )
        ),
    ).build_payloads()
    assert result.payloads == ()


def test_adapter_executes_select_only_with_connection_factory() -> None:
    class SpyCursor:
        def __init__(self, queries: list[str]) -> None:
            self.queries = queries
            self.description: tuple[tuple[str], ...] = ()

        def execute(self, sql: str) -> SpyCursor:
            self.queries.append(sql)
            if " LIMIT 0" in sql:
                columns = CO_REQUIRED
            else:
                selected = sql.removeprefix("SELECT ").split(" FROM ", maxsplit=1)[0]
                columns = tuple(part.strip() for part in selected.split(","))
            self.description = tuple((column,) for column in columns)
            return self

        def fetchall(self) -> list[tuple[Any, ...]]:
            return []

        def close(self) -> None:
            return None

    class SpyConnection:
        def __init__(self) -> None:
            self.queries: list[str] = []

        def cursor(self) -> SpyCursor:
            return SpyCursor(self.queries)

        def close(self) -> None:
            return None

    connection = SpyConnection()
    adapter = ReadOnlyMartAdapter(lambda: connection)

    assert adapter.fund_co_holdings() == ()

    assert connection.queries
    assert all(
        query.lstrip().upper().startswith("SELECT ")
        for query in connection.queries
    )
    assert not any(
        query.lstrip().upper().startswith(("CREATE ", "INSERT ", "UPDATE ", "DELETE "))
        for query in connection.queries
    )


def _create_all_tables(connection: duckdb.DuckDBPyConnection) -> None:
    _create_fund_co_table(connection)
    _create_northbound_table(connection)
    _create_top_holder_table(connection)
    for table in (
        FUND_CO_HOLDING_LINEAGE_MART,
        NORTHBOUND_Z_SCORE_LINEAGE_MART,
        TOP_HOLDER_QOQ_LINEAGE_MART,
    ):
        _create_lineage_table(connection, table)


def _create_fund_co_table(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        f"""
        CREATE TABLE {FUND_CO_HOLDING_MART} (
            mart_key VARCHAR,
            row_id VARCHAR,
            report_date VARCHAR,
            security_id_left VARCHAR,
            security_id_right VARCHAR,
            co_holding_fund_count INTEGER,
            security_left_fund_count INTEGER,
            security_right_fund_count INTEGER,
            jaccard_score DOUBLE,
            latest_announced_date VARCHAR,
            evidence_ref VARCHAR
        )
        """
    )


def _create_northbound_table(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        f"""
        CREATE TABLE {NORTHBOUND_Z_SCORE_MART} (
            mart_key VARCHAR,
            row_id VARCHAR,
            security_id VARCHAR,
            holder_id VARCHAR,
            report_date VARCHAR,
            z_score_metric VARCHAR,
            lookback_observations INTEGER,
            window_start_date VARCHAR,
            window_end_date VARCHAR,
            observation_count INTEGER,
            metric_value DOUBLE,
            metric_mean DOUBLE,
            metric_stddev DOUBLE,
            metric_z_score DOUBLE,
            evidence_ref VARCHAR
        )
        """
    )


def _create_top_holder_table(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        f"""
        CREATE TABLE {TOP_HOLDER_QOQ_MART} (
            mart_key VARCHAR,
            holding_source VARCHAR,
            holder_id VARCHAR,
            security_id VARCHAR,
            report_date VARCHAR,
            announced_date VARCHAR,
            previous_report_date VARCHAR,
            previous_announced_date VARCHAR,
            holding_amount DOUBLE,
            previous_holding_amount DOUBLE,
            holding_amount_delta DOUBLE,
            holding_amount_delta_pct DOUBLE,
            holding_ratio DOUBLE,
            previous_holding_ratio DOUBLE,
            holding_ratio_delta DOUBLE
        )
        """
    )


def _create_lineage_table(connection: duckdb.DuckDBPyConnection, table: str) -> None:
    connection.execute(
        f"""
        CREATE TABLE {table} (
            mart_key VARCHAR,
            dataset VARCHAR,
            snapshot_id VARCHAR,
            as_of_date VARCHAR,
            source_mart VARCHAR,
            source_window_start_date VARCHAR,
            source_window_end_date VARCHAR,
            source_interface_ids VARCHAR[],
            source_row_count INTEGER,
            source_lineage_row_count INTEGER,
            source_lineage_summary VARCHAR,
            source_run_ids VARCHAR[],
            raw_loaded_at_min VARCHAR,
            raw_loaded_at_max VARCHAR
        )
        """
    )


def _insert_normal_rows(connection: duckdb.DuckDBPyConnection) -> None:
    _insert_co_holding(connection)
    connection.execute(
        (
            f"INSERT INTO {NORTHBOUND_Z_SCORE_MART} "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        ),
        [
            "nb-key",
            "northbound-alpha",
            "security-alpha",
            "northbound-holder",
            "2026-03-31",
            "holding_ratio",
            8,
            "2025-12-31",
            "2026-03-31",
            63,
            0.018,
            0.011,
            0.0029,
            2.4,
            "evidence-northbound-alpha",
        ],
    )
    connection.execute(
        (
            f"INSERT INTO {TOP_HOLDER_QOQ_MART} "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        ),
        [
            "top-key",
            "top_holder",
            "holder-alpha",
            "security-alpha",
            "2026-03-31",
            "2026-04-30",
            "2025-12-31",
            "2026-01-31",
            4200000.0,
            3900000.0,
            300000.0,
            0.0769,
            0.042,
            0.031,
            0.011,
        ],
    )
    _insert_lineage(
        connection,
        FUND_CO_HOLDING_LINEAGE_MART,
        "co-key",
        "run-co",
        "fund-position rows aggregated into security-pair overlap",
    )
    _insert_lineage(
        connection,
        NORTHBOUND_Z_SCORE_LINEAGE_MART,
        "nb-key",
        "run-nb",
        "windowed holding-ratio observations standardized per holder-security",
    )
    _insert_lineage(
        connection,
        TOP_HOLDER_QOQ_LINEAGE_MART,
        "top-key",
        "run-top",
        "current and previous report-date rows joined by holder-security",
    )


def _insert_co_holding(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        (
            f"INSERT INTO {FUND_CO_HOLDING_MART} "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        ),
        [
            "co-key",
            "coholding-alpha",
            "2026-03-31",
            "security-alpha",
            "security-beta",
            12,
            30,
            24,
            0.286,
            "2026-04-30",
            "evidence-coholding-alpha",
        ],
    )


def _insert_lineage(
    connection: duckdb.DuckDBPyConnection,
    table: str,
    mart_key: str,
    run_id: str,
    summary: str,
) -> None:
    connection.execute(
        f"INSERT INTO {table} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            mart_key,
            "holdings_derivation_mart",
            "snapshot-alpha",
            "2026-03-31",
            "mart_fact_holding_position_v2",
            "2025-12-31",
            "2026-03-31",
            ["fund-position-summary"],
            54,
            12,
            summary,
            [run_id],
            "2026-04-30T00:00:00Z",
            "2026-04-30T00:05:00Z",
        ],
    )
