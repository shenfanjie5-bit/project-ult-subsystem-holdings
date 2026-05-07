# Holdings Producer Runbook

This runbook is for the gated holdings producer and production queue submit
runner in this repo. It is an operations guide for readiness and bounded
execute checks only.

## Scope Guardrails

- Submit only existing `Ex3CandidateGraphDelta` candidate deltas.
- Submit only `CO_HOLDING` and `NORTHBOUND_HOLD` relation types from this
  producer.
- Keep `mart_deriv_top_holder_qoq_change` as read-only audit input. Top
  shareholder and pledge status must not become new holdings relation types;
  when those semantics are represented in graph data they belong on
  `OWNERSHIP` properties outside this producer submission path.
- Do not add subtypes, create financial-doc output, add
  `MAJOR_CUSTOMER` or `MAJOR_SUPPLIER`, or claim default propagation, full
  propagation, or broad rollout completion from this repo.
- When `--scope-manifest` is provided, use canonical manifest target refs plus
  two-hop context refs only. Two-hop associated companies are graph/risk context;
  they must not be counted as decision targets.

## Entity Registry Fixture And Alias Map

Production readiness and execute require holder and security source ids to
resolve to canonical `ENT_` entity refs before queue submit. The runner accepts
an explicit JSON fixture through either `--entity-registry-fixture` or
`--entity-registry-alias-map`. Provide only one of those flags.

Accepted JSON shapes:

```json
{
  "aliases": {
    "security-alpha": "ENT_SECURITY_ALPHA",
    "security-beta": "ENT_SECURITY_BETA",
    "northbound-holder": "ENT_NORTHBOUND_HOLDER"
  },
  "entity_refs": [
    "ENT_SECURITY_ALPHA",
    "ENT_SECURITY_BETA",
    "ENT_NORTHBOUND_HOLDER"
  ]
}
```

```json
{
  "alias_map": [
    {"alias": "security-alpha", "ref": "ENT_SECURITY_ALPHA"},
    {"alias_text": "security-beta", "canonical_entity_id": "ENT_SECURITY_BETA"},
    {"alias": "northbound-holder", "entity_id": "ENT_NORTHBOUND_HOLDER"}
  ],
  "entities": [
    {"ref": "ENT_SECURITY_ALPHA"},
    {"ref": "ENT_SECURITY_BETA"},
    {"ref": "ENT_NORTHBOUND_HOLDER"}
  ]
}
```

Handling rules:

- Alias strings are trimmed, then matched exactly. Do not rely on case folding,
  ticker normalization, market suffix inference, or deriving aliases from
  `entity_refs`.
- Every alias target must be a non-empty canonical ref that starts with `ENT_`.
- Every alias target must also appear in `entity_refs`, `refs`, or `entities`.
- A duplicate alias is allowed only if it points to the same canonical ref.
  Conflicting duplicate aliases fail closed with
  `entity_registry_fixture_ambiguous_alias`.
- Missing refs, invalid JSON, invalid canonical refs, conflicting fixture
  inputs, or a fixture plus injected entity lookup fail closed before
  readiness.

## Holdings Scope Manifest

The runner can constrain producer output with `--scope-manifest`. This is a
small sanitized JSON file that names canonical manifest targets and optional
two-hop context refs. It is not a publish manifest and must not contain raw
provider rows, paths, aliases, tokens, or local execution details.

Accepted JSON shape:

```json
{
  "manifest_targets": ["ENT_SECURITY_ALPHA"],
  "two_hop_context_entity_refs": ["ENT_SECURITY_BETA"]
}
```

Equivalent nested shapes under `holdings_scope` or `scope` are accepted when
the values are canonical `ENT_` refs or objects with `ref`, `entity_id`, or
`canonical_entity_id`. Alias strings such as `security-alpha` fail closed with
`holdings_scope_manifest_invalid_ref`.

Filtering rules:

- `CO_HOLDING` rows are submitted only when both security endpoints are inside
  the manifest target plus two-hop context set.
- `NORTHBOUND_HOLD` rows are submitted only when the target security is inside
  that set. The northbound holder source is graph context and never becomes a
  decision target.
- Rows filtered by scope are not submitted. They are counted through
  `audit_counts.scope_filtered` and `scope_filtered_payload_count`.
- Included payloads carry only role metadata in `producer_context`; summaries
  expose counts only: `scope_configured`, `scope_manifest_target_count`,
  `scope_two_hop_context_count`, `scope_allowed_entity_count`,
  `scope_decision_target_payload_count`, and
  `scope_graph_risk_context_payload_count`.

## Readiness Command

Use readiness first. It builds producer payloads, runs production entity
preflight, validates summary counters, and does not submit to the queue.

```bash
python scripts/run_production_queue_submit.py \
  --duckdb-path <verified-holdings-marts.duckdb> \
  --mode readiness \
  --entity-registry-fixture <entity-registry-fixture.json> \
  --scope-manifest <holdings-scope.json> \
  --summary-json <sanitized-summary.json>
```

`--duckdb-path` can also come from `DP_DUCKDB_PATH`. The summary is sanitized:
it must not contain local paths, DSNs, raw payloads, raw provider identifiers,
tokens, stdout, stderr, exitcode, manifests, parquet paths, or concrete
`delta_id` values. Do not commit runtime summaries or proof artifacts.

## Execute Command

Execute only after readiness returns `ready=true`. Execute is gated by
`SUBSYSTEM_HOLDINGS_PRODUCTION_QUEUE_SUBMIT_CONFIRM=1`, and bounded canary
submits with `--max-payloads` require `--allow-partial-submit`.

```bash
SUBSYSTEM_HOLDINGS_PRODUCTION_QUEUE_SUBMIT_CONFIRM=1 \
python scripts/run_production_queue_submit.py \
  --duckdb-path <verified-holdings-marts.duckdb> \
  --mode execute \
  --max-payloads <positive-count> \
  --allow-partial-submit \
  --entity-registry-fixture <entity-registry-fixture.json> \
  --scope-manifest <holdings-scope.json> \
  --summary-json <sanitized-summary.json>
```

Execute requires the idempotent data-platform queue API. If the SDK or backend
does not expose an idempotent submit path, the runner returns
`data_platform_queue_idempotent_submit_unavailable`, records zero submit
receipts, and must be treated as a failed closed result.

## Idempotency And Delta Id Guidance

Producer `delta_id` values are deterministic from the mart row id:

- `holdings-co-<row_id>` for `CO_HOLDING`.
- `holdings-nb-<row_id>` for `NORTHBOUND_HOLD`.

Do not include timestamps, run ids, local file paths, or queue receipt ids in
`delta_id`. On rerun, the same mart row should produce the same `delta_id`; the
external `--run-id` is only an operator correlation id and is not a `delta_id`
override. Readiness summaries intentionally omit concrete `delta_id` values, so
operators should compare payload counts, relation counts, readiness reasons,
and sanitized receipt counters instead of copying private ids into evidence.

## Fail-Closed Audit Interpretation

Treat `ready=false` as the source of truth. Start with `reason`, then inspect
`reasons`, `audit_counts`, `adapter_diagnostic_counts`,
`submit_mart_diagnostic_count`, `top_holder_diagnostic_count`, and preflight
counters.

Common interpretations:

- `no_payloads`: no submittable payloads were built. If `audit_counts` includes
  `unresolved_holder` or `unresolved_security`, fix entity registry aliases or
  refs first.
- `unresolved_alignments_exceeded`: holder or security resolution exceeded the
  configured threshold. The production default is zero unresolved alignments.
- `submit_mart_diagnostics_exceeded`: blocking derivation mart diagnostics
  exist for `mart_deriv_fund_co_holding`,
  `mart_deriv_lineage_fund_co_holding`,
  `mart_deriv_northbound_holding_z_score`, or
  `mart_deriv_lineage_northbound_holding_z_score`.
- `adapter_schema_mismatch`: required mart columns are missing or malformed.
  The runner returns a sanitized failure summary and emits no payloads.
- `production_preflight_blocked` or `selected_production_preflight_blocked`:
  entity preflight rejected unresolved source or target refs before backend
  submit.
- `data_platform_queue_idempotent_submit_unavailable`: execute did not submit
  because the required idempotent backend path is unavailable.
- `scope_filtered`: producer rows were outside the manifest target plus
  two-hop context scope. Check only the sanitized scope counters; do not copy
  manifest contents or entity refs into evidence.

`read_only_input` audit records and `top_holder_diagnostic_count` are not submit
blockers by default because top-holder rows are audit-only. They still require
operator review, and they do not prove an ownership edge or new relation type.

## Local Verification

```bash
python -m pytest -q tests/boundary
python -m pytest -q tests/contract/test_production_queue_submit_runner.py
python -m pytest -q tests/contract/test_data_platform_queue_submit.py
python -m pytest -q tests/contract/test_ex3_payload_and_submit.py
git diff --check
```
