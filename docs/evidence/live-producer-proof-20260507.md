# Live producer proof - 2026-05-07

## Scope

- Module: `subsystem-holdings`
- Source commit under test: `362ad8c91a0e46070aa02ed4530e291b7bc1af41`
  (`main`, PR #6 merge commit)
- Evidence type: bounded live producer proof status after the data-platform
  live holdings backfill attempt.
- Secret handling: no token values, DSNs, concrete stock codes, concrete fund
  codes, raw provider payloads, local runtime paths, or dbt logs are recorded
  here.

## Boundary

The intended proof boundary was:

1. Read live-built DuckDB holdings marts through
   `ReadOnlyMartAdapter.from_duckdb_path(DP_DUCKDB_PATH)`.
2. Use deterministic proof-only entity mappings derived from mart rows.
3. Build in-memory Ex-3 candidate payloads.
4. Validate each payload against the existing `Ex3CandidateGraphDelta`
   contract.
5. Do not submit to a queue.

This evidence does not claim production queue behavior, live graph
propagation, graph-engine #55 behavior, contracts subtype changes, or
financial-doc/M4.7 behavior.

## Upstream Blocker

The upstream data-platform bounded live holdings backfill did not complete.

Exact upstream blocker:

- `AdapterFetchError`
- Provider fetch failed for `source_id='tushare'`,
  `asset_id='tushare_top10_holders'`.
- Error summary: `HTTPConnectionPool(host='api.waditu.com', port=80): Read
  timed out. (read timeout=30)`.

The plan-only backfill preflight did pass with `planned_count=5`,
`skipped_count=0`, and `rejected_count=0`, but no complete live Raw Zone
holdings input set was produced.

## Producer Proof Status

Status: `BLOCKED`.

`ReadOnlyMartAdapter.from_duckdb_path(DP_DUCKDB_PATH)` was not run against a
live-built holdings mart database for this proof, because the required DuckDB
tables were not built from complete live holdings inputs.

Required live-built tables not proven in this run:

- `mart_fact_holding_position_v2`
- `mart_deriv_top_holder_qoq_change`
- `mart_deriv_fund_co_holding`
- `mart_deriv_northbound_holding_z_score`
- `mart_deriv_lineage_top_holder_qoq_change`
- `mart_deriv_lineage_fund_co_holding`
- `mart_deriv_lineage_northbound_holding_z_score`

No producer payloads were built from live marts, and no queue submit path was
called.

## Relation Boundary

The existing producer remains scoped to these relation types when a live mart
database is available:

- `CO_HOLDING`
- `NORTHBOUND_HOLD`

Top-holder quarter-over-quarter rows remain audit-only and are not submitted.

## Command Shape

Expected proof-only producer command shape once the live DuckDB mart database
exists:

```bash
DP_DUCKDB_PATH='<runtime-duckdb>' \
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:../contracts/src \
  .venv/bin/python '<proof-only-snippet-or-test>' \
  # read-only adapter, deterministic injected mappings, build payloads only
```

This command shape was not executed against live marts in this blocked run.

## Result

The live producer proof remains blocked by upstream live holdings backfill
availability. No contracts files were changed, no graph-engine #55 work was
entered, and no production queue or graph propagation behavior is claimed.
