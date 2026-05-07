# project-ult-subsystem-holdings

Offline-first and gated production-readiness producer for holdings-derived Ex-3
candidate deltas.

## Current Status

Status: bounded gated canary/live production evidence `PASS`.

Current landed scope:

- Read only canonical and derivation mart-shaped holdings rows.
- Use an in-memory fake mart reader for offline proof and tests.
- Use `ReadOnlyMartAdapter` for already-landed data-platform mart tables.
- Fail closed when holder or security entity alignment is unresolved.
- Emit only existing `Ex3CandidateGraphDelta` payloads.
- Keep the submitted relation whitelist to `CO_HOLDING` and `NORTHBOUND_HOLD`.
- Map `mart_deriv_fund_co_holding` security-pair rows to `CO_HOLDING` between the two security entities. The fake row keeps `report_date`, `security_id_left`, `security_id_right`, fund counts, `jaccard_score`, and `latest_announced_date`.
- Map `mart_deriv_northbound_holding_z_score` rows to `NORTHBOUND_HOLD` between the holder and security entities. The fake row keeps the `security_id`, `holder_id`, `report_date`, `z_score_metric`, fixed `lookback_observations=8`, `window_start_date`, `window_end_date`, `observation_count`, metric stats, and `metric_z_score`.
- Keep `mart_deriv_top_holder_qoq_change` rows as read-only audit input. Top-holder rows are not submitted as relationship candidates. Audit records retain the full mart row fields and expanded derivation lineage summary.
- Preserve provider-neutral derivation lineage in emitted payload properties, evidence, and producer context using data-platform field names: `source_mart`, `source_interface_ids`, `source_lineage_row_count`, `source_lineage_summary`, `source_run_ids`, `raw_loaded_at_min`, and `raw_loaded_at_max`. Producer context keeps the derivation mart and source shape separate from lineage source mart.
- Keep `FakeHoldingsMartReader` as the offline fixture reader.

Bounded live producer proof now shows a complete live holdings backfill, dbt
mart build/test pass, required mart and lineage tables present and non-empty,
and 55 in-memory Ex-3 candidate payloads validated against the existing
contract: `CO_HOLDING=45` and `NORTHBOUND_HOLD=10`.

Production hardening guards have landed for the gated production submit path:

- Readiness is the default mode; execute mode requires explicit confirmation.
- Execute canary submission is bounded by `--max-payloads` and requires explicit partial-submit allowance.
- Production entity preflight runs before queue submit.
- Missing DuckDB targets, unresolved entity alignment, blocking adapter diagnostics, missing lineage, unsafe receipts, and missing idempotent queue APIs fail closed.
- Summaries and evidence are sanitized and do not record secrets, DSNs, concrete market/fund codes, raw provider payloads, or local runtime paths.

## Next Step

The next operational step is post-canary operationalization and runbook
hardening before any controlled opt-in/default propagation canary. That work
should preserve the current producer boundary and keep default/full propagation
disabled until explicitly approved and evidenced.

Operational runbook: [Holdings Producer Runbook](docs/producer-runbook.md).

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
- This repo does not claim default or full propagation is enabled.
- This repo does not claim broad production rollout is complete.
- This repo does not claim a production entity registry or M4.8 runtime is fully complete.
- This repo does not claim M4.7 or financial-doc completion.
- This repo does not claim contracts subtype changes.

## Local Checks

```bash
python -m pytest -q
python -m pytest -q tests/boundary
python -m pytest -q tests/contract/test_production_queue_submit_runner.py
python -m pytest -q tests/contract/test_data_platform_queue_submit.py
python -m pytest -q tests/contract/test_ex3_payload_and_submit.py
git diff --check origin/main...HEAD
```

## Evidence

- [Read-only mart adapter proof, 2026-05-06](docs/evidence/read-only-mart-adapter-proof-20260506.md)
- [Live producer proof blockers, 2026-05-06](docs/evidence/live-producer-proof-blockers-20260506.md)
- [Live producer proof, 2026-05-07](docs/evidence/live-producer-proof-20260507.md)
- [Live producer proof attempt2 PASS, 2026-05-07](docs/evidence/live-producer-proof-20260507-attempt2.md)
