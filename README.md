# project-ult-subsystem-holdings

Offline-first producer scaffold for holdings-derived Ex-3 candidate deltas.

PR1 scope:

- Read only canonical and derivation mart-shaped holdings rows.
- Use an in-memory fake mart reader for proof and tests.
- Fail closed when holder or security entity alignment is unresolved.
- Emit only existing `Ex3CandidateGraphDelta` payloads.
- Map `mart_deriv_fund_co_holding` security-pair rows to `CO_HOLDING` between the two security entities. The fake row keeps `report_date`, `security_id_left`, `security_id_right`, fund counts, `jaccard_score`, and `latest_announced_date`.
- Map `mart_deriv_northbound_holding_z_score` rows to `NORTHBOUND_HOLD` between the holder and security entities. The fake row keeps the `security_id`, `holder_id`, `report_date`, `z_score_metric`, fixed `lookback_observations=8`, `window_start_date`, `window_end_date`, `observation_count`, metric stats, and `metric_z_score`.
- Keep `mart_deriv_top_holder_qoq_change` rows as read-only PR1 input; PR1 does not submit top-holder relationship candidates. Audit records retain the full mart row fields and expanded derivation lineage summary.
- Preserve provider-neutral derivation lineage in emitted payload properties, evidence, and producer context using data-platform field names: `source_mart`, `source_interface_ids`, `source_lineage_row_count`, `source_lineage_summary`, `source_run_ids`, `raw_loaded_at_min`, and `raw_loaded_at_max`. Producer context keeps the derivation mart and source shape separate from lineage source mart.
- Keep `FakeHoldingsMartReader` as the offline fixture reader.

## Read-Only Mart Adapter

`ReadOnlyMartAdapter` reads already-landed data-platform mart tables through injected configuration. It supports either a DB-API style `connection_factory` or `ReadOnlyMartAdapter.from_duckdb_path(database_path)`, which opens DuckDB in read-only mode. The adapter does not read `.env` files, does not hard-code a path or connection string, and only executes `SELECT` statements.

Allowed tables:

- `mart_fact_holding_position_v2`
- `mart_deriv_top_holder_qoq_change`
- `mart_deriv_fund_co_holding`
- `mart_deriv_northbound_holding_z_score`
- `mart_deriv_lineage_top_holder_qoq_change`
- `mart_deriv_lineage_fund_co_holding`
- `mart_deriv_lineage_northbound_holding_z_score`

Fail-closed behavior:

- Missing table: return an empty sequence and append an adapter diagnostic with `reason="missing_table"`.
- Empty result: return empty sequences; the producer emits zero payloads.
- Schema mismatch: raise `AdapterSchemaError` and append `reason="schema_mismatch"` so callers do not partially submit malformed rows.
- Missing lineage: skip the affected derivation row and append `reason="missing_lineage"`. Co-holding and northbound rows are never submitted without paired lineage.

Example:

```python
from subsystem_holdings import ReadOnlyMartAdapter

reader = ReadOnlyMartAdapter.from_duckdb_path(configured_database_path)
```

Boundary:

- This repo does not define new holdings subtypes.
- This repo does not connect to live market data providers, staging tables, queue transports, or downstream graph services. The real adapter reads landed mart tables only.
- Top shareholder and pledge semantics remain ownership properties, not new relation types.

## Local Checks

```bash
python -m pytest -q
git diff --check origin/main...HEAD
```
