from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

import pytest

from subsystem_holdings.entity_registry_adapter import EntityRegistryAdapter
from subsystem_holdings.models import FundCoHoldingRow, LineageSummary
from subsystem_holdings.producer import HoldingsProducer
from subsystem_holdings.reader import FakeHoldingsMartReader, build_default_fake_reader


@dataclass(frozen=True, slots=True)
class RegistryEntity:
    canonical_entity_id: str


class RecordingEntityRefsLookup:
    def __init__(self, result: Mapping[str, bool]) -> None:
        self._result = dict(result)
        self.calls: list[tuple[str, ...]] = []

    def __call__(self, refs: Iterable[str]) -> Mapping[str, bool]:
        refs_tuple = tuple(refs)
        self.calls.append(refs_tuple)
        return {ref: self._result.get(ref, False) for ref in refs_tuple}


def test_registry_alias_alignment_builds_canonical_ent_payloads() -> None:
    alias_map = {
        "security-alpha": RegistryEntity("ENT_SECURITY_ALPHA_CANONICAL"),
        "security-beta": RegistryEntity("ENT_SECURITY_BETA_CANONICAL"),
        "northbound-holder": RegistryEntity("ENT_HOLDER_NORTHBOUND_CANONICAL"),
    }
    adapter = EntityRegistryAdapter(lookup_alias_func=alias_map.get)

    result = HoldingsProducer(build_default_fake_reader(), adapter).build_payloads()
    payloads = {payload["relation_type"]: payload for payload in result.payloads}

    assert result.audit[0].reason == "read_only_input"
    assert payloads["CO_HOLDING"]["source_node"] == "ENT_SECURITY_ALPHA_CANONICAL"
    assert payloads["CO_HOLDING"]["target_node"] == "ENT_SECURITY_BETA_CANONICAL"
    assert payloads["NORTHBOUND_HOLD"]["source_node"] == (
        "ENT_HOLDER_NORTHBOUND_CANONICAL"
    )
    assert payloads["NORTHBOUND_HOLD"]["target_node"] == (
        "ENT_SECURITY_ALPHA_CANONICAL"
    )


@pytest.mark.parametrize(
    ("alias_result", "expected_reason"),
    (
        (None, "registry_alias_miss"),
        ([], "registry_alias_ambiguous"),
        (
            [RegistryEntity("ENT_SECURITY_ALPHA"), RegistryEntity("ENT_SECURITY_BETA")],
            "registry_alias_ambiguous",
        ),
        ({}, "registry_alias_missing_canonical_id"),
        ({"canonical_entity_id": "NOT_ENT"}, "invalid_canonical_entity_id"),
    ),
)
def test_registry_alias_unresolved_cases_fail_closed(
    alias_result: Any,
    expected_reason: str,
) -> None:
    adapter = EntityRegistryAdapter(lookup_alias_func=lambda _: alias_result)

    decision = adapter.security("security-alpha")

    assert decision.resolved is False
    assert decision.reason == expected_reason
    assert decision.metadata is not None
    assert decision.metadata["entity_role"] == "security"


def test_registry_alias_exception_fails_closed_with_diagnostics() -> None:
    def raise_lookup(_: str) -> object:
        raise RuntimeError("registry offline")

    decision = EntityRegistryAdapter(lookup_alias_func=raise_lookup).holder(
        "holder-alpha"
    )

    assert decision.resolved is False
    assert decision.reason == "registry_lookup_exception"
    assert decision.metadata == {
        "entity_role": "holder",
        "error_type": "RuntimeError",
        "error": "registry offline",
    }


def test_registry_audit_detail_carries_unresolved_diagnostics() -> None:
    lineage = LineageSummary(
        dataset="holdings_canonical_mart",
        snapshot_id="snapshot-registry",
        as_of_date="2026-03-31",
    )
    reader = FakeHoldingsMartReader(
        co_holdings=(
            FundCoHoldingRow(
                row_id="coholding-registry-miss",
                report_date="2026-03-31",
                security_id_left="security-missing",
                security_id_right="security-beta",
                co_holding_fund_count=8,
                security_left_fund_count=19,
                security_right_fund_count=21,
                jaccard_score=0.25,
                latest_announced_date="2026-04-30",
                evidence_ref="evidence-registry-miss",
                lineage=lineage,
            ),
        )
    )
    adapter = EntityRegistryAdapter(lookup_alias_func=lambda _: None)

    result = HoldingsProducer(reader, adapter).build_payloads()

    assert result.payloads == ()
    assert len(result.audit) == 1
    audit = result.audit[0]
    assert audit.reason == "unresolved_security"
    assert audit.detail == {
        "source_id": "security-missing",
        "alignment_reason": "registry_alias_miss",
        "alignment_metadata": {"entity_role": "security"},
    }


def test_existing_ent_refs_require_registry_lookup_true() -> None:
    lookup = RecordingEntityRefsLookup({"ENT_SECURITY_ALPHA": True})
    adapter = EntityRegistryAdapter(
        lookup_alias_func=lambda _: pytest.fail("ENT ref must not use alias lookup"),
        lookup_entity_refs_func=lookup,
    )

    decision = adapter.security("ENT_SECURITY_ALPHA")

    assert decision.resolved is True
    assert decision.node_id == "ENT_SECURITY_ALPHA"
    assert decision.reason == "registry_ref_verified"
    assert lookup.calls == [("ENT_SECURITY_ALPHA",)]


def test_existing_ent_refs_fail_closed_when_lookup_false() -> None:
    lookup = RecordingEntityRefsLookup({"ENT_SECURITY_ALPHA": False})
    adapter = EntityRegistryAdapter(lookup_entity_refs_func=lookup)

    decision = adapter.security("ENT_SECURITY_ALPHA")

    assert decision.resolved is False
    assert decision.reason == "registry_ref_unresolved"
    assert lookup.calls == [("ENT_SECURITY_ALPHA",)]


def test_adapter_lookup_supports_sdk_entity_preflight_shape() -> None:
    lookup = RecordingEntityRefsLookup({"ENT_A": True, "ENT_B": False})
    adapter = EntityRegistryAdapter(lookup_entity_refs_func=lookup)

    assert adapter.lookup(["ENT_A", "ENT_B", "ENT_C"]) == {
        "ENT_A": True,
        "ENT_B": False,
        "ENT_C": False,
    }
