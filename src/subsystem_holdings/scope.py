from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from subsystem_holdings.errors import ScopeManifestError

ScopeRole = Literal["manifest_target", "two_hop_context", "outside_scope"]
ScopeUsage = Literal["decision_target", "graph_risk_context"]

_TARGET_KEYS = (
    "manifest_targets",
    "target_entity_refs",
    "target_entities",
    "decision_targets",
    "targets",
)
_CONTEXT_KEYS = (
    "two_hop_context",
    "two_hop_context_entity_refs",
    "context_entity_refs",
    "context_entities",
    "associated_company_refs",
)
_REF_KEYS = (
    "ref",
    "entity_ref",
    "entity_id",
    "canonical_entity_id",
    "canonical_id",
    "node_id",
)


@dataclass(frozen=True, slots=True)
class ScopeCheck:
    allowed: bool
    reason: str
    usage: ScopeUsage | None
    source_role: ScopeRole
    target_role: ScopeRole

    def as_context(self) -> dict[str, object]:
        if self.usage is None:
            raise ValueError("scope context is only available for allowed payloads")
        return {
            "decision_usage": self.usage,
            "source_role": self.source_role,
            "target_role": self.target_role,
            "manifest_target_matched": (
                self.source_role == "manifest_target"
                or self.target_role == "manifest_target"
            ),
            "two_hop_context_matched": (
                self.source_role == "two_hop_context"
                or self.target_role == "two_hop_context"
            ),
        }


@dataclass(frozen=True, slots=True)
class HoldingsScope:
    target_entity_refs: frozenset[str]
    two_hop_context_entity_refs: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        if not self.target_entity_refs:
            raise ScopeManifestError("holdings_scope_manifest_missing_targets")
        for ref in self.allowed_entity_refs:
            _require_entity_ref(ref)

    @property
    def allowed_entity_refs(self) -> frozenset[str]:
        return self.target_entity_refs | self.two_hop_context_entity_refs

    def role_for_ref(self, ref: str | None) -> ScopeRole:
        if ref in self.target_entity_refs:
            return "manifest_target"
        if ref in self.two_hop_context_entity_refs:
            return "two_hop_context"
        return "outside_scope"

    def evaluate(
        self,
        *,
        relation_type: str,
        source_node: str,
        target_node: str,
    ) -> ScopeCheck:
        source_role = self.role_for_ref(source_node)
        target_role = self.role_for_ref(target_node)
        if relation_type == "CO_HOLDING":
            if source_role == "outside_scope" or target_role == "outside_scope":
                return ScopeCheck(
                    allowed=False,
                    reason="company_endpoint_outside_scope",
                    usage=None,
                    source_role=source_role,
                    target_role=target_role,
                )
            return ScopeCheck(
                allowed=True,
                reason="in_scope",
                usage=_usage_from_roles(source_role, target_role),
                source_role=source_role,
                target_role=target_role,
            )
        if relation_type == "NORTHBOUND_HOLD":
            if target_role == "outside_scope":
                return ScopeCheck(
                    allowed=False,
                    reason="target_outside_scope",
                    usage=None,
                    source_role=source_role,
                    target_role=target_role,
                )
            return ScopeCheck(
                allowed=True,
                reason="in_scope",
                usage=_usage_from_roles(target_role),
                source_role=source_role,
                target_role=target_role,
            )
        return ScopeCheck(
            allowed=False,
            reason="relation_type_outside_scope",
            usage=None,
            source_role=source_role,
            target_role=target_role,
        )


def load_scope_manifest(path: Path) -> HoldingsScope:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ScopeManifestError("holdings_scope_manifest_unreadable") from exc
    except json.JSONDecodeError as exc:
        raise ScopeManifestError("holdings_scope_manifest_invalid_json") from exc
    if not isinstance(payload, Mapping):
        raise ScopeManifestError("holdings_scope_manifest_invalid_shape")
    return holdings_scope_from_manifest(payload)


def holdings_scope_from_manifest(payload: Mapping[str, Any]) -> HoldingsScope:
    target_refs = _refs_for_keys(payload, _TARGET_KEYS)
    if not target_refs:
        raise ScopeManifestError("holdings_scope_manifest_missing_targets")
    context_refs = _refs_for_keys(payload, _CONTEXT_KEYS)
    return HoldingsScope(
        target_entity_refs=frozenset(target_refs),
        two_hop_context_entity_refs=frozenset(context_refs),
    )


def _refs_for_keys(
    payload: Mapping[str, Any],
    keys: Sequence[str],
) -> tuple[str, ...]:
    raw_values: list[Any] = []
    for key in keys:
        if key in payload:
            raw_values.append(payload[key])
    for container_key in ("scope", "holdings_scope"):
        nested = payload.get(container_key)
        if isinstance(nested, Mapping):
            for key in keys:
                if key in nested:
                    raw_values.append(nested[key])
    refs = {
        _require_entity_ref(ref)
        for raw in raw_values
        for ref in _iter_refs(raw)
    }
    return tuple(sorted(refs))


def _iter_refs(raw: Any) -> tuple[str, ...]:
    if raw is None:
        return ()
    if isinstance(raw, str):
        return (raw,)
    if isinstance(raw, Mapping):
        for key in _REF_KEYS:
            if key in raw:
                return _iter_refs(raw[key])
        for nested_key in ("refs", "entity_refs", "entities"):
            if nested_key in raw:
                return _iter_refs(raw[nested_key])
        raise ScopeManifestError("holdings_scope_manifest_invalid_ref")
    if isinstance(raw, Sequence) and not isinstance(raw, (bytes, bytearray)):
        refs: list[str] = []
        for item in raw:
            refs.extend(_iter_refs(item))
        return tuple(refs)
    raise ScopeManifestError("holdings_scope_manifest_invalid_ref")


def _require_entity_ref(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ScopeManifestError("holdings_scope_manifest_invalid_ref")
    ref = value.strip()
    if not ref.startswith("ENT_"):
        raise ScopeManifestError("holdings_scope_manifest_invalid_ref")
    return ref


def _usage_from_roles(*roles: ScopeRole) -> ScopeUsage:
    if "manifest_target" in roles:
        return "decision_target"
    return "graph_risk_context"


__all__ = [
    "HoldingsScope",
    "ScopeCheck",
    "ScopeRole",
    "ScopeUsage",
    "holdings_scope_from_manifest",
    "load_scope_manifest",
]
