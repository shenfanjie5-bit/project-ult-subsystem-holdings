# project-ult-subsystem-holdings

Offline-first producer scaffold for holdings-derived Ex-3 candidate deltas.

PR1 scope:

- Read only canonical and derivation mart-shaped holdings rows.
- Use an in-memory fake mart reader for proof and tests.
- Fail closed when holder or security entity alignment is unresolved.
- Emit only existing `Ex3CandidateGraphDelta` payloads.
- Map fund co-holding rows to `CO_HOLDING`.
- Map northbound z-score rows to `NORTHBOUND_HOLD`.
- Keep top-holder quarter-over-quarter rows as read-only PR1 input; PR1 does not submit top-holder relationship candidates.

Boundary:

- This repo does not define new holdings subtypes.
- This repo does not connect to live market data providers, staging tables, queue transports, or downstream graph services.
- Top shareholder and pledge semantics remain ownership properties, not new relation types.

## Local Checks

```bash
python -m pytest -q
git diff --check
```
