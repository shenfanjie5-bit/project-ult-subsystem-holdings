from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, Protocol

from subsystem_holdings.errors import AlignmentError
from subsystem_holdings.models import AlignmentDecision


class EntityAlignmentResolver(Protocol):
    def holder(self, holder_id: str) -> AlignmentDecision: ...

    def security(self, security_id: str) -> AlignmentDecision: ...


@dataclass(frozen=True, slots=True)
class EntityAlignmentTable:
    holder_nodes: Mapping[str, str] = field(default_factory=dict)
    security_nodes: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for table_name, table in (
            ("holder_nodes", self.holder_nodes),
            ("security_nodes", self.security_nodes),
        ):
            for source_id, node_id in table.items():
                if not source_id.strip() or not node_id.strip():
                    raise AlignmentError(f"{table_name} cannot contain blank ids")


@dataclass(frozen=True, slots=True)
class EntityAligner:
    table: EntityAlignmentTable

    def holder(self, holder_id: str) -> AlignmentDecision:
        return AlignmentDecision(holder_id, self.table.holder_nodes.get(holder_id))

    def security(self, security_id: str) -> AlignmentDecision:
        return AlignmentDecision(security_id, self.table.security_nodes.get(security_id))


def build_default_aligner() -> EntityAligner:
    return EntityAligner(
        EntityAlignmentTable(
            holder_nodes={
                "fund-alpha": "ENT_FUND_ALPHA",
                "fund-beta": "ENT_FUND_BETA",
                "holder-alpha": "ENT_HOLDER_ALPHA",
                "northbound-holder": "ENT_NORTHBOUND_HOLDER",
            },
            security_nodes={
                "security-alpha": "ENT_SECURITY_ALPHA",
                "security-beta": "ENT_SECURITY_BETA",
            },
        )
    )
