from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from subsystem_holdings.models import AlignmentDecision

LookupAliasFunc = Callable[[str], Any]
LookupEntityRefsFunc = Callable[[Iterable[str]], Mapping[str, bool]]


@dataclass(frozen=True, slots=True)
class EntityRegistryAdapter:
    lookup_alias_func: LookupAliasFunc | None = None
    lookup_entity_refs_func: LookupEntityRefsFunc | None = None

    def holder(self, holder_id: str) -> AlignmentDecision:
        return self._resolve_alias(holder_id, entity_role="holder")

    def security(self, security_id: str) -> AlignmentDecision:
        return self._resolve_alias(security_id, entity_role="security")

    def lookup(self, refs: Iterable[str]) -> Mapping[str, bool]:
        refs_tuple = tuple(refs)
        lookup_entity_refs = self._lookup_entity_refs_func()
        result = lookup_entity_refs(refs_tuple)
        if not isinstance(result, Mapping):
            raise RuntimeError("entity-registry lookup_entity_refs returned non-mapping")
        return {ref: result.get(ref) is True for ref in refs_tuple}

    def _resolve_alias(self, source_id: str, *, entity_role: str) -> AlignmentDecision:
        if _is_canonical_entity_id(source_id):
            return self._verify_canonical_ref(source_id, entity_role=entity_role)

        try:
            alias_result = self._lookup_alias_func()(source_id)
        except Exception as exc:  # noqa: BLE001 - resolution must fail closed.
            return _unresolved(
                source_id,
                "registry_lookup_exception",
                entity_role=entity_role,
                error_type=type(exc).__name__,
                error=str(exc),
            )

        canonical_id, reason, metadata = _extract_canonical_id(alias_result)
        if canonical_id is None:
            return _unresolved(
                source_id,
                reason,
                entity_role=entity_role,
                **metadata,
            )
        if not _is_canonical_entity_id(canonical_id):
            return _unresolved(
                source_id,
                "invalid_canonical_entity_id",
                entity_role=entity_role,
                canonical_entity_id=canonical_id,
            )
        return AlignmentDecision(
            source_id=source_id,
            node_id=canonical_id,
            reason="registry_alias_resolved",
            metadata={"entity_role": entity_role},
        )

    def _verify_canonical_ref(
        self,
        source_id: str,
        *,
        entity_role: str,
    ) -> AlignmentDecision:
        try:
            resolved = self.lookup((source_id,)).get(source_id) is True
        except Exception as exc:  # noqa: BLE001 - resolution must fail closed.
            return _unresolved(
                source_id,
                "registry_ref_lookup_exception",
                entity_role=entity_role,
                error_type=type(exc).__name__,
                error=str(exc),
            )
        if resolved:
            return AlignmentDecision(
                source_id=source_id,
                node_id=source_id,
                reason="registry_ref_verified",
                metadata={"entity_role": entity_role},
            )
        return _unresolved(
            source_id,
            "registry_ref_unresolved",
            entity_role=entity_role,
        )

    def _lookup_alias_func(self) -> LookupAliasFunc:
        if self.lookup_alias_func is not None:
            return self.lookup_alias_func

        try:
            from entity_registry import lookup_alias
        except ImportError as exc:
            raise RuntimeError(
                "entity-registry is not importable; configure the runtime with "
                "entity-registry on PYTHONPATH or pass lookup_alias_func"
            ) from exc

        return lookup_alias

    def _lookup_entity_refs_func(self) -> LookupEntityRefsFunc:
        if self.lookup_entity_refs_func is not None:
            return self.lookup_entity_refs_func

        try:
            from entity_registry import lookup_entity_refs
        except ImportError as exc:
            raise RuntimeError(
                "entity-registry is not importable; configure the runtime with "
                "entity-registry on PYTHONPATH or pass lookup_entity_refs_func"
            ) from exc

        return lookup_entity_refs


def _is_canonical_entity_id(value: str) -> bool:
    return isinstance(value, str) and value.strip() == value and value.startswith("ENT_")


def _unresolved(source_id: str, reason: str, **metadata: object) -> AlignmentDecision:
    return AlignmentDecision(
        source_id=source_id,
        node_id=None,
        reason=reason,
        metadata=metadata,
    )


def _extract_canonical_id(value: Any) -> tuple[str | None, str, dict[str, object]]:
    if value is None:
        return None, "registry_alias_miss", {}

    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        if len(value) != 1:
            return None, "registry_alias_ambiguous", {"candidate_count": len(value)}
        value = value[0]

    canonical_id = _candidate_canonical_id(value)
    if canonical_id is None:
        return None, "registry_alias_missing_canonical_id", {
            "result_type": type(value).__name__,
        }
    return canonical_id, "registry_alias_resolved", {}


def _candidate_canonical_id(value: Any) -> str | None:
    if isinstance(value, Mapping):
        for key in ("canonical_entity_id", "entity_id", "canonical_id"):
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()
        return None

    for attribute in ("canonical_entity_id", "entity_id", "canonical_id"):
        candidate = getattr(value, attribute, None)
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
    return None


__all__ = [
    "EntityRegistryAdapter",
    "LookupAliasFunc",
    "LookupEntityRefsFunc",
]
