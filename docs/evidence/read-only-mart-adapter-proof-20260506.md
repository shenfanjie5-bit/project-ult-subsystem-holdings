# Read-Only Mart Adapter Proof

Date: 2026-05-06

Scope: PR #3 / commit `752c9842d75048a12b28472ed55c83615f7bd199`.

## Summary

This evidence records the non-live read-only mart adapter proof for subsystem-holdings. The producer scaffold exists, and a real `ReadOnlyMartAdapter` exists alongside the fixture reader used by the local proof. The proof is non-live, fixture-backed, and limited to local DuckDB/read-only SQL behavior.

The adapter proof shows that already-landed mart rows can be read through a read-only adapter, fed into the existing producer scaffold, and validated against the existing Ex-3 payload contract without changing contracts, subtypes, provider access, queue behavior, or graph propagation.

## Proof Boundary

- Non-live proof only.
- Fixture-backed local DuckDB input only.
- Read-only SQL only; adapter queries are `SELECT` statements.
- DuckDB adapter entrypoint opens the database in read-only mode.
- No live producer execution was proven.
- No live holdings backfill was performed.
- No live provider call is made from subsystem-holdings.
- No production queue or live graph propagation was proven.
- No graph-engine #55 implementation is included.
- No M4.7 or M4.8 closure is claimed.
- No contracts or subtype change is included.

## Proof Points

- `ReadOnlyMartAdapter` reads a fixture-backed mart database and feeds the existing producer scaffold.
- Ex-3 payload validation passes for emitted candidate graph deltas.
- Missing mart tables fail closed by returning no rows and recording adapter diagnostics.
- Schema mismatch fails closed by raising adapter schema errors and recording diagnostics.
- Missing lineage fails closed; affected derivation rows are skipped rather than emitted without lineage.
- Adapter SQL is SELECT-only in the local proof.
- DuckDB is opened with read-only behavior for path-based adapter construction.
- Top-holder quarter-over-quarter rows remain read-only audit input and are not submitted as relationship candidates.
- Emitted relation types are limited to `CO_HOLDING` and `NORTHBOUND_HOLD`.

## Checks Run

The final review used the repository-supported Python environment and ran:

```bash
.venv/bin/python -m pytest -q
.venv/bin/python -m pytest -q tests/unit/test_read_only_mart_adapter.py
.venv/bin/python -m pytest -q tests/contract/test_ex3_payload_and_submit.py
.venv/bin/python -m pytest -q tests/unit/test_fake_reader_and_alignment.py
.venv/bin/python -m pytest -q tests/boundary
git diff --check origin/main...HEAD
```

All checks passed for this evidence PR.

## Hygiene Review

This evidence is documentation-only. It intentionally excludes:

- Tokens or secrets.
- DSNs or connection strings.
- Concrete market codes or fund codes.
- Raw provider payloads.
- Local absolute paths.
- Temporary filesystem paths.
- Runtime logs.

## Not Done

- No live producer proof.
- No live holdings backfill.
- No live provider call from subsystem-holdings.
- No production queue or live graph propagation proof.
- No graph-engine #55 implementation.
- No M4.7 or M4.8 closure.
- No contracts or subtype change.
