# project-ult-subsystem-holdings

Offline-first producer scaffold for holdings-derived Ex-3 candidate deltas.

PR1 scope:

- Read only canonical and derivation mart-shaped holdings rows.
- Use an in-memory fake mart reader for proof and tests.
- Fail closed when holder or security entity alignment is unresolved.
- Emit only existing `Ex3CandidateGraphDelta` payloads.
- Map `mart_deriv_fund_co_holding` security-pair rows to `CO_HOLDING` between the two security entities. The fake row keeps `report_date`, `security_id_left`, `security_id_right`, fund counts, `jaccard_score`, and `latest_announced_date`.
- Map `mart_deriv_northbound_holding_z_score` rows to `NORTHBOUND_HOLD` between the holder and security entities. The fake row keeps the `security_id`, `holder_id`, `report_date`, `z_score_metric`, `lookback_observations`, `window_start_date`, `window_end_date`, `observation_count`, metric stats, and `metric_z_score`.
- Keep top-holder quarter-over-quarter rows as read-only PR1 input; PR1 does not submit top-holder relationship candidates.

Boundary:

- This repo does not define new holdings subtypes.
- This repo does not connect to live market data providers, staging tables, queue transports, or downstream graph services.
- Top shareholder and pledge semantics remain ownership properties, not new relation types.

## Local Checks

```bash
python -m pytest -q
git diff --check origin/main...HEAD
```
