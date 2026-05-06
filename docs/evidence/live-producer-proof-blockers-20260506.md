# Live Producer Proof Blockers

Date: 2026-05-06

Scope: live producer proof readiness after PR #4 / commit `41ac033125fab3f810b98ad5e7ad5c5606ae227a`.

## Summary

This evidence records the exact blockers preventing the next live producer proof for subsystem-holdings.

The previously merged proof is the non-live read-only adapter proof. It established fixture-backed and read-only mart adapter behavior only. The next step is blocked because the environment is not intentionally unlocked for live holdings backfill and no available DuckDB or database contains all required holdings mart tables.

No live proof work was executed for this evidence.

## Readiness Result

- `DP_TUSHARE_TOKEN=SET/redacted`
- `DP_TUSHARE_LIVE_HOLDINGS_SMOKE=SET`
- `DP_TUSHARE_LIVE_HOLDINGS_BACKFILL=missing`
- `DP_PG_DSN=SET/redacted`
- `DATABASE_URL=SET/redacted`
- `DP_DUCKDB_PATH=SET but configured target missing`
- `DP_ICEBERG_WAREHOUSE_PATH=SET target exists`
- `DP_RAW_ZONE_PATH=SET target exists`
- `DP_PROCESSED_DATA_PATH=SET target missing`
- `DP_DATA_STORAGE_ROOT_PATH=SET target missing`
- `DP_ICEBERG_CATALOG_NAME=SET/redacted`
- No existing DuckDB or database contains all required holdings mart tables.

## Required Holdings Mart Tables

- `mart_fact_holding_position_v2`
- `mart_deriv_top_holder_qoq_change`
- `mart_deriv_fund_co_holding`
- `mart_deriv_northbound_holding_z_score`
- `mart_deriv_lineage_top_holder_qoq_change`
- `mart_deriv_lineage_fund_co_holding`
- `mart_deriv_lineage_northbound_holding_z_score`

## Blockers

- Live holdings backfill is not intentionally enabled because `DP_TUSHARE_LIVE_HOLDINGS_BACKFILL` is missing.
- The configured DuckDB target is missing.
- Available database targets do not contain the complete required holdings mart table set.
- Processed data and data storage root targets are missing.
- Without a complete holdings mart database, the live producer proof cannot read the required landed mart and lineage inputs.

## Explicitly Not Done

- No live producer proof was executed.
- No live backfill was executed.
- No provider call was made.
- No production queue or live graph proof was executed.
- No graph-engine #55 work was executed.
- No contracts or subtype change was made.

## Next Unlock Conditions

To unblock the live producer proof:

1. Intentionally set `DP_TUSHARE_LIVE_HOLDINGS_BACKFILL`.
2. Provide or read a holdings mart database containing all required mart and lineage tables listed above.
3. Run a bounded live/backfill/mart build only after the live gate and target database are intentionally prepared.
4. Run the bounded producer proof against the prepared holdings mart database.

## Checks Run For This Docs PR

```bash
docs-only diff check
.venv/bin/python -m pytest -q tests/boundary
git diff --check origin/main...HEAD
hygiene scan for secrets, DSNs, concrete codes, raw payloads, local paths, temporary paths, and logs
```

All checks passed for this docs-only evidence PR.

## Hygiene Review

This evidence is documentation-only. It intentionally excludes:

- Token values or secrets.
- DSNs or connection strings.
- Concrete market codes or fund codes.
- Raw provider payloads.
- Local absolute paths.
- Temporary filesystem paths.
- Runtime logs.
