from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pytest

from subsystem_holdings.alignment import EntityAligner, EntityAlignmentTable
from subsystem_holdings.errors import AdapterSchemaError
from subsystem_holdings.mart_adapter import (
    FACT_HOLDING_POSITION_MART,
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


def test_data_platform_post_fix_schema_reads_adapter_and_feeds_producer(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "data_platform_post_fix.duckdb"
    expected_tables = {
        FACT_HOLDING_POSITION_MART,
        TOP_HOLDER_QOQ_MART,
        FUND_CO_HOLDING_MART,
        NORTHBOUND_Z_SCORE_MART,
        TOP_HOLDER_QOQ_LINEAGE_MART,
        FUND_CO_HOLDING_LINEAGE_MART,
        NORTHBOUND_Z_SCORE_LINEAGE_MART,
    }
    connection = duckdb.connect(str(db_path))
    try:
        _create_all_tables(connection)
        _insert_normal_rows(connection)
        actual_tables = {
            row[0]
            for row in connection.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'main'
                """
            ).fetchall()
        }
    finally:
        connection.close()

    assert expected_tables == actual_tables

    adapter = ReadOnlyMartAdapter.from_duckdb_path(db_path)

    canonical_positions = adapter.canonical_positions()
    assert len(canonical_positions) == 1
    assert canonical_positions[0].position_id == "position-alpha"
    assert canonical_positions[0].lineage.as_properties() == {
        "dataset": "holding_position",
        "snapshot_id": "holding_position:2026-03-31",
        "as_of_date": "2026-04-30",
        "source_mart": FACT_HOLDING_POSITION_MART,
    }

    top_holder_changes = adapter.top_holder_qoq_changes()
    assert len(top_holder_changes) == 1
    assert top_holder_changes[0].lineage.as_properties()["source_interface_ids"] == [
        "top10_holders"
    ]
    assert top_holder_changes[0].lineage.as_properties()["source_run_ids"] == [
        "run-top"
    ]
    assert len(adapter.fund_co_holdings()) == 1
    assert len(adapter.northbound_z_scores()) == 1

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
    assert [payload["delta_id"] for payload in result.payloads] == [
        "holdings-co-coholding-alpha",
        "holdings-nb-northbound-alpha",
    ]
    assert result.audit[0].reason == "read_only_input"
    assert result.audit[0].detail["lineage"]["source_run_ids"] == ["run-top"]
    assert result.payloads[0]["producer_context"]["lineage"]["source_run_ids"] == [
        "run-co"
    ]
    assert result.payloads[0]["producer_context"]["lineage"][
        "source_interface_ids"
    ] == ["fund_portfolio"]
    assert result.payloads[1]["producer_context"]["lineage"]["source_run_ids"] == [
        "run-nb"
    ]
    assert result.payloads[1]["producer_context"]["lineage"][
        "source_interface_ids"
    ] == ["hsgt_hold_top10"]
    assert adapter.diagnostics == []


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


def test_incomplete_top_holder_qoq_row_fails_closed(tmp_path: Path) -> None:
    db_path = tmp_path / "incomplete_top_holder.duckdb"
    connection = duckdb.connect(str(db_path))
    try:
        _create_top_holder_table(connection)
        _create_lineage_table(connection, TOP_HOLDER_QOQ_LINEAGE_MART)
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
                None,
                None,
                4200000.0,
                None,
                None,
                None,
                0.042,
                None,
                None,
            ],
        )
        _insert_lineage(
            connection,
            TOP_HOLDER_QOQ_LINEAGE_MART,
            "top-key",
            "run-top",
            "first observed report-date row has no previous comparison row",
        )
    finally:
        connection.close()
    adapter = ReadOnlyMartAdapter.from_duckdb_path(db_path)

    rows = adapter.top_holder_qoq_changes()

    assert rows == ()
    assert len(adapter.diagnostics) == 1
    assert adapter.diagnostics[0].table == TOP_HOLDER_QOQ_MART
    assert adapter.diagnostics[0].reason == "incomplete_change_row"
    assert adapter.diagnostics[0].row_key == "top-key"


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
    _create_fact_holding_position_table(connection)
    _create_fund_co_table(connection)
    _create_northbound_table(connection)
    _create_top_holder_table(connection)
    for table in (
        FUND_CO_HOLDING_LINEAGE_MART,
        NORTHBOUND_Z_SCORE_LINEAGE_MART,
        TOP_HOLDER_QOQ_LINEAGE_MART,
    ):
        _create_lineage_table(connection, table)


def _create_fact_holding_position_table(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    connection.execute(
        f"""
        CREATE TABLE {FACT_HOLDING_POSITION_MART} (
            position_id VARCHAR,
            holding_source VARCHAR,
            holder_id VARCHAR,
            holder_name VARCHAR,
            holder_type VARCHAR,
            security_id VARCHAR,
            report_date DATE,
            announced_date DATE,
            holding_amount DECIMAL(38, 18),
            holding_ratio DECIMAL(38, 18),
            holding_float_ratio DECIMAL(38, 18),
            holding_change DECIMAL(38, 18),
            market_value DECIMAL(38, 18),
            exchange VARCHAR,
            dataset VARCHAR,
            snapshot_id VARCHAR,
            as_of_date DATE
        )
        """
    )


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
    business_key_columns = {
        TOP_HOLDER_QOQ_LINEAGE_MART: """
            holding_source VARCHAR,
            holder_id VARCHAR,
            security_id VARCHAR,
            report_date DATE,
            announced_date DATE,
        """,
        FUND_CO_HOLDING_LINEAGE_MART: """
            report_date DATE,
            security_id_left VARCHAR,
            security_id_right VARCHAR,
        """,
        NORTHBOUND_Z_SCORE_LINEAGE_MART: """
            security_id VARCHAR,
            holder_id VARCHAR,
            report_date DATE,
            z_score_metric VARCHAR,
        """,
    }[table]
    connection.execute(
        f"""
        CREATE TABLE {table} (
            mart_key VARCHAR,
            {business_key_columns}
            dataset VARCHAR,
            snapshot_id VARCHAR,
            as_of_date DATE,
            source_mart VARCHAR,
            source_window_start_date DATE,
            source_window_end_date DATE,
            source_interface_ids VARCHAR,
            source_row_count INTEGER,
            source_lineage_row_count INTEGER,
            source_run_ids VARCHAR,
            raw_loaded_at_min TIMESTAMP,
            raw_loaded_at_max TIMESTAMP,
            source_lineage_summary VARCHAR
        )
        """
    )


def _insert_normal_rows(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        (
            f"INSERT INTO {FACT_HOLDING_POSITION_MART} "
            """
            (
                position_id,
                holding_source,
                holder_id,
                holder_name,
                holder_type,
                security_id,
                report_date,
                announced_date,
                holding_amount,
                holding_ratio,
                holding_float_ratio,
                holding_change,
                market_value,
                exchange,
                dataset,
                snapshot_id,
                as_of_date
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        ),
        [
            "position-alpha",
            "fund_portfolio",
            "fund-alpha",
            "Fund Alpha",
            "fund",
            "security-alpha",
            "2026-03-31",
            "2026-04-30",
            4200000.0,
            0.042,
            0.042,
            None,
            176400.0,
            None,
            "holding_position",
            "holding_position:2026-03-31",
            "2026-04-30",
        ],
    )
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
    business_columns: tuple[str, ...]
    business_values: list[Any]
    if table == TOP_HOLDER_QOQ_LINEAGE_MART:
        business_columns = (
            "holding_source",
            "holder_id",
            "security_id",
            "report_date",
            "announced_date",
        )
        business_values = [
            "top_holder",
            "holder-alpha",
            "security-alpha",
            "2026-03-31",
            "2026-04-30",
        ]
        dataset = "top_holder_qoq_change"
    elif table == FUND_CO_HOLDING_LINEAGE_MART:
        business_columns = (
            "report_date",
            "security_id_left",
            "security_id_right",
        )
        business_values = [
            "2026-03-31",
            "security-alpha",
            "security-beta",
        ]
        dataset = "fund_co_holding"
    elif table == NORTHBOUND_Z_SCORE_LINEAGE_MART:
        business_columns = (
            "security_id",
            "holder_id",
            "report_date",
            "z_score_metric",
        )
        business_values = [
            "security-alpha",
            "northbound-holder",
            "2026-03-31",
            "holding_ratio",
        ]
        dataset = "northbound_holding_z_score"
    else:
        raise AssertionError(f"unexpected lineage table: {table}")

    source_interface_ids = {
        TOP_HOLDER_QOQ_LINEAGE_MART: "top10_holders",
        FUND_CO_HOLDING_LINEAGE_MART: "fund_portfolio",
        NORTHBOUND_Z_SCORE_LINEAGE_MART: "hsgt_hold_top10",
    }[table]
    columns = (
        ("mart_key",)
        + business_columns
        + (
            "dataset",
            "snapshot_id",
            "as_of_date",
            "source_mart",
            "source_window_start_date",
            "source_window_end_date",
            "source_interface_ids",
            "source_row_count",
            "source_lineage_row_count",
            "source_run_ids",
            "raw_loaded_at_min",
            "raw_loaded_at_max",
            "source_lineage_summary",
        )
    )
    placeholders = ", ".join("?" for _ in columns)
    connection.execute(
        f"""
        INSERT INTO {table} ({", ".join(columns)})
        VALUES ({placeholders})
        """,
        [
            mart_key,
            *business_values,
            dataset,
            f"{dataset}:{mart_key}",
            "2026-04-30",
            "mart_fact_holding_position_v2",
            "2025-12-31",
            "2026-03-31",
            source_interface_ids,
            54,
            12,
            run_id,
            "2026-04-30T00:00:00Z",
            "2026-04-30T00:05:00Z",
            summary,
        ],
    )
