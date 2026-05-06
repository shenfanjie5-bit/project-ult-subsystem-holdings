from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from subsystem_holdings.errors import AdapterSchemaError
from subsystem_holdings.models import (
    CanonicalHoldingPosition,
    FundCoHoldingRow,
    LineageSummary,
    NorthboundZScoreRow,
    TopHolderQoQChange,
)

FACT_HOLDING_POSITION_MART = "mart_fact_holding_position_v2"
TOP_HOLDER_QOQ_MART = "mart_deriv_top_holder_qoq_change"
FUND_CO_HOLDING_MART = "mart_deriv_fund_co_holding"
NORTHBOUND_Z_SCORE_MART = "mart_deriv_northbound_holding_z_score"

TOP_HOLDER_QOQ_LINEAGE_MART = "mart_deriv_lineage_top_holder_qoq_change"
FUND_CO_HOLDING_LINEAGE_MART = "mart_deriv_lineage_fund_co_holding"
NORTHBOUND_Z_SCORE_LINEAGE_MART = (
    "mart_deriv_lineage_northbound_holding_z_score"
)


class ConnectionFactory(Protocol):
    def __call__(self) -> Any: ...


@dataclass(frozen=True, slots=True)
class AdapterDiagnostic:
    table: str
    reason: str
    detail: str
    row_key: str | None = None


@dataclass(slots=True)
class ReadOnlyMartAdapter:
    connection_factory: ConnectionFactory
    diagnostics: list[AdapterDiagnostic] = field(default_factory=list)

    @classmethod
    def from_duckdb_path(cls, database_path: str | Path) -> ReadOnlyMartAdapter:
        path = Path(database_path)

        def connect() -> Any:
            import duckdb

            return duckdb.connect(str(path), read_only=True)

        return cls(connection_factory=connect)

    def clear_diagnostics(self) -> None:
        self.diagnostics.clear()

    def canonical_positions(self) -> Sequence[CanonicalHoldingPosition]:
        rows = self._read_table(
            table=FACT_HOLDING_POSITION_MART,
            required=(
                "position_id",
                "holder_id",
                "security_id",
                "report_date",
                "holding_ratio",
                "dataset",
                "snapshot_id",
                "as_of_date",
            ),
            order_by=("position_id",),
        )
        return tuple(
            CanonicalHoldingPosition(
                position_id=str(row["position_id"]),
                holder_id=str(row["holder_id"]),
                security_id=str(row["security_id"]),
                report_date=str(row["report_date"]),
                holding_ratio=float(row["holding_ratio"]),
                lineage=LineageSummary(
                    dataset=str(row["dataset"]),
                    snapshot_id=str(row["snapshot_id"]),
                    as_of_date=str(row["as_of_date"]),
                    source_mart=FACT_HOLDING_POSITION_MART,
                ),
            )
            for row in rows
        )

    def top_holder_qoq_changes(self) -> Sequence[TopHolderQoQChange]:
        rows = self._read_table(
            table=TOP_HOLDER_QOQ_MART,
            required=(
                "mart_key",
                "holding_source",
                "holder_id",
                "security_id",
                "report_date",
                "announced_date",
                "previous_report_date",
                "previous_announced_date",
                "holding_amount",
                "previous_holding_amount",
                "holding_amount_delta",
                "holding_amount_delta_pct",
                "holding_ratio",
                "previous_holding_ratio",
                "holding_ratio_delta",
            ),
            order_by=("mart_key",),
        )
        if not rows:
            return ()

        lineages = self._lineage_by_key(TOP_HOLDER_QOQ_LINEAGE_MART)
        result: list[TopHolderQoQChange] = []
        for row in rows:
            lineage = self._paired_lineage(TOP_HOLDER_QOQ_MART, row, lineages)
            if lineage is None:
                continue
            result.append(
                TopHolderQoQChange(
                    holding_source=str(row["holding_source"]),
                    holder_id=str(row["holder_id"]),
                    security_id=str(row["security_id"]),
                    report_date=str(row["report_date"]),
                    announced_date=str(row["announced_date"]),
                    previous_report_date=str(row["previous_report_date"]),
                    previous_announced_date=str(row["previous_announced_date"]),
                    holding_amount=float(row["holding_amount"]),
                    previous_holding_amount=float(row["previous_holding_amount"]),
                    holding_amount_delta=float(row["holding_amount_delta"]),
                    holding_amount_delta_pct=float(row["holding_amount_delta_pct"]),
                    holding_ratio=float(row["holding_ratio"]),
                    previous_holding_ratio=float(row["previous_holding_ratio"]),
                    holding_ratio_delta=float(row["holding_ratio_delta"]),
                    lineage=lineage,
                )
            )
        return tuple(result)

    def fund_co_holdings(self) -> Sequence[FundCoHoldingRow]:
        rows = self._read_table(
            table=FUND_CO_HOLDING_MART,
            required=(
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
            ),
            order_by=("mart_key",),
        )
        if not rows:
            return ()

        lineages = self._lineage_by_key(FUND_CO_HOLDING_LINEAGE_MART)
        result: list[FundCoHoldingRow] = []
        for row in rows:
            lineage = self._paired_lineage(FUND_CO_HOLDING_MART, row, lineages)
            if lineage is None:
                continue
            result.append(
                FundCoHoldingRow(
                    row_id=str(row["row_id"]),
                    report_date=str(row["report_date"]),
                    security_id_left=str(row["security_id_left"]),
                    security_id_right=str(row["security_id_right"]),
                    co_holding_fund_count=int(row["co_holding_fund_count"]),
                    security_left_fund_count=int(row["security_left_fund_count"]),
                    security_right_fund_count=int(row["security_right_fund_count"]),
                    jaccard_score=float(row["jaccard_score"]),
                    latest_announced_date=str(row["latest_announced_date"]),
                    evidence_ref=str(row["evidence_ref"]),
                    lineage=lineage,
                )
            )
        return tuple(result)

    def northbound_z_scores(self) -> Sequence[NorthboundZScoreRow]:
        rows = self._read_table(
            table=NORTHBOUND_Z_SCORE_MART,
            required=(
                "mart_key",
                "row_id",
                "security_id",
                "holder_id",
                "report_date",
                "z_score_metric",
                "lookback_observations",
                "window_start_date",
                "window_end_date",
                "observation_count",
                "metric_value",
                "metric_mean",
                "metric_stddev",
                "metric_z_score",
                "evidence_ref",
            ),
            order_by=("mart_key",),
        )
        if not rows:
            return ()

        lineages = self._lineage_by_key(NORTHBOUND_Z_SCORE_LINEAGE_MART)
        result: list[NorthboundZScoreRow] = []
        for row in rows:
            lineage = self._paired_lineage(NORTHBOUND_Z_SCORE_MART, row, lineages)
            if lineage is None:
                continue
            result.append(
                NorthboundZScoreRow(
                    row_id=str(row["row_id"]),
                    security_id=str(row["security_id"]),
                    holder_id=str(row["holder_id"]),
                    report_date=str(row["report_date"]),
                    z_score_metric=str(row["z_score_metric"]),  # type: ignore[arg-type]
                    lookback_observations=int(row["lookback_observations"]),
                    window_start_date=str(row["window_start_date"]),
                    window_end_date=str(row["window_end_date"]),
                    observation_count=int(row["observation_count"]),
                    metric_value=float(row["metric_value"]),
                    metric_mean=float(row["metric_mean"]),
                    metric_stddev=float(row["metric_stddev"]),
                    metric_z_score=float(row["metric_z_score"]),
                    evidence_ref=str(row["evidence_ref"]),
                    lineage=lineage,
                )
            )
        return tuple(result)

    def _lineage_by_key(self, table: str) -> dict[str, LineageSummary]:
        rows = self._read_table(
            table=table,
            required=(
                "mart_key",
                "dataset",
                "snapshot_id",
                "as_of_date",
                "source_mart",
                "source_window_start_date",
                "source_window_end_date",
                "source_interface_ids",
                "source_row_count",
                "source_lineage_row_count",
                "source_lineage_summary",
                "source_run_ids",
                "raw_loaded_at_min",
                "raw_loaded_at_max",
            ),
            order_by=("mart_key",),
        )
        return {
            str(row["mart_key"]): LineageSummary(
                dataset=str(row["dataset"]),
                snapshot_id=str(row["snapshot_id"]),
                as_of_date=str(row["as_of_date"]),
                source_mart=_optional_str(row["source_mart"]),
                source_window_start_date=_optional_str(row["source_window_start_date"]),
                source_window_end_date=_optional_str(row["source_window_end_date"]),
                source_interface_ids=_string_tuple(row["source_interface_ids"]),
                source_row_count=_optional_int(row["source_row_count"]),
                source_lineage_row_count=_optional_int(row["source_lineage_row_count"]),
                source_lineage_summary=_optional_str(row["source_lineage_summary"]),
                source_run_ids=_string_tuple(row["source_run_ids"]),
                raw_loaded_at_min=_optional_str(row["raw_loaded_at_min"]),
                raw_loaded_at_max=_optional_str(row["raw_loaded_at_max"]),
            )
            for row in rows
        }

    def _paired_lineage(
        self,
        table: str,
        row: Mapping[str, Any],
        lineages: Mapping[str, LineageSummary],
    ) -> LineageSummary | None:
        mart_key = str(row["mart_key"])
        lineage = lineages.get(mart_key)
        if lineage is not None:
            return lineage
        self.diagnostics.append(
            AdapterDiagnostic(
                table=table,
                reason="missing_lineage",
                detail="derivation row has no paired lineage row",
                row_key=mart_key,
            )
        )
        return None

    def _read_table(
        self,
        *,
        table: str,
        required: Sequence[str],
        order_by: Sequence[str] = (),
    ) -> tuple[dict[str, Any], ...]:
        columns = self._table_columns(table)
        if columns is None:
            return ()
        missing = tuple(column for column in required if column not in columns)
        if missing:
            detail = f"missing required columns: {', '.join(missing)}"
            self.diagnostics.append(
                AdapterDiagnostic(table=table, reason="schema_mismatch", detail=detail)
            )
            raise AdapterSchemaError(f"{table} {detail}")

        column_sql = ", ".join(required)
        order_sql = f" ORDER BY {', '.join(order_by)}" if order_by else ""
        return self._select_rows(f"SELECT {column_sql} FROM {table}{order_sql}")

    def _table_columns(self, table: str) -> set[str] | None:
        try:
            rows = self._select_rows(f"SELECT * FROM {table} LIMIT 0")
        except Exception as exc:
            if _looks_like_missing_table(exc):
                self.diagnostics.append(
                    AdapterDiagnostic(
                        table=table,
                        reason="missing_table",
                        detail="configured mart table is unavailable",
                    )
                )
                return None
            raise
        if not rows:
            return self._last_columns
        return set(rows[0])

    _last_columns: set[str] = field(default_factory=set, init=False)

    def _select_rows(self, sql: str) -> tuple[dict[str, Any], ...]:
        if not sql.lstrip().upper().startswith("SELECT "):
            raise AssertionError("ReadOnlyMartAdapter only executes SELECT statements")

        connection = self.connection_factory()
        cursor = None
        try:
            cursor = connection.cursor() if hasattr(connection, "cursor") else connection
            result = cursor.execute(sql)
            active_cursor = result if hasattr(result, "fetchall") else cursor
            description = active_cursor.description or ()
            columns = {str(column[0]) for column in description}
            self._last_columns = columns
            records = active_cursor.fetchall()
            column_names = [str(column[0]) for column in description]
            return tuple(
                dict(zip(column_names, record, strict=True)) for record in records
            )
        finally:
            if (
                cursor is not None
                and cursor is not connection
                and hasattr(cursor, "close")
            ):
                cursor.close()
            if hasattr(connection, "close"):
                connection.close()


def _looks_like_missing_table(exc: Exception) -> bool:
    message = str(exc).lower()
    return "no such table" in message or "does not exist" in message


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _string_tuple(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return ()
        return tuple(part.strip() for part in text.split(",") if part.strip())
    if isinstance(value, Iterable):
        return tuple(str(item) for item in value)
    return (str(value),)
