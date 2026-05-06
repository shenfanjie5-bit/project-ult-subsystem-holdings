# Live producer proof - 2026-05-07 attempt2

## Scope

- Module: `subsystem-holdings`
- Evidence type: curated bounded live producer proof after live holdings
  backfill and mart build.
- Status: `PASS`.
- Secret handling: no token values, DSNs, concrete stock codes, concrete fund
  codes, raw provider payloads, local runtime paths, or dbt logs are recorded
  here.

## Boundary

The proof boundary was:

1. Verify live holdings backfill completion and artifact shape.
2. Verify required dbt mart build and test results.
3. Verify required holdings mart and lineage tables are present and non-empty.
4. Read the live-built DuckDB holdings marts through the read-only mart adapter.
5. Use deterministic proof-only entity mappings derived from mart rows.
6. Build in-memory Ex-3 candidate payloads.
7. Validate each payload against the existing `Ex3CandidateGraphDelta`
   contract.
8. Do not submit to a queue.

This evidence does not claim production queue behavior, live graph propagation,
graph-engine #55 behavior, contracts subtype changes, or financial-doc/M4.7
behavior.

## Live Backfill And Mart Build

- Live backfill execution: ok.
- Live backfill `artifact_count=5`.
- Live backfill `row_count=32`.
- `dbt run`: `12` success.
- `dbt test`: `118` pass.
- Required holdings mart tables present and non-empty: `7`.
- Missing required columns across required tables: `0`.

Required holdings mart tables verified present and non-empty:

- `mart_fact_holding_position_v2`
- `mart_deriv_top_holder_qoq_change`
- `mart_deriv_fund_co_holding`
- `mart_deriv_northbound_holding_z_score`
- `mart_deriv_lineage_top_holder_qoq_change`
- `mart_deriv_lineage_fund_co_holding`
- `mart_deriv_lineage_northbound_holding_z_score`

## Mart Row Counts

- `mart_fact_holding_position_v2`: `36`
- `mart_deriv_top_holder_qoq_change`: `20`
- `mart_deriv_fund_co_holding`: `45`
- `mart_deriv_northbound_holding_z_score`: `10`
- `mart_deriv_lineage_top_holder_qoq_change`: `20`
- `mart_deriv_lineage_fund_co_holding`: `45`
- `mart_deriv_lineage_northbound_holding_z_score`: `10`

## Lineage And Key Parity

- Top-holder QoQ lineage rows: `20`.
- Fund co-holding lineage rows: `45`.
- Northbound holding z-score lineage rows: `10`.
- `mart_key` parity mismatches for top-holder QoQ: `0`.
- `mart_key` parity mismatches for fund co-holding: `0`.
- `mart_key` parity mismatches for northbound holding z-score: `0`.
- `mart_key` parity: `0` for all producer-read derivation tables.

## Producer Payload Result

- `payload_count=55`.
- `relation_counts`: `CO_HOLDING=45`, `NORTHBOUND_HOLD=10`.

Top-holder quarter-over-quarter rows remain read-only audit input and are not
submitted as relationship candidates. Incomplete top-holder QoQ rows fail
closed in the adapter: affected rows are skipped and an adapter diagnostic is
recorded with reason `incomplete_change_row`.

## Explicitly Not Claimed

- No production queue submit behavior is claimed.
- No live graph propagation behavior is claimed.
- No graph-engine #55 behavior is claimed.
- No contracts subtype change is claimed.
- No financial-doc/M4.7 behavior is claimed.

## Hygiene Review

This curated evidence intentionally excludes:

- Token values or secrets.
- DSNs or connection strings.
- Concrete market codes or fund codes.
- Raw provider payloads.
- Local absolute runtime paths.
- dbt logs.
