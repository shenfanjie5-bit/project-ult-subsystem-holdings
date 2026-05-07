from __future__ import annotations

from subsystem_holdings.alignment import (
    EntityAligner,
    EntityAlignmentResolver,
    EntityAlignmentTable,
    build_default_aligner,
)
from subsystem_holdings.entity_registry_adapter import EntityRegistryAdapter
from subsystem_holdings.mart_adapter import AdapterDiagnostic, ReadOnlyMartAdapter
from subsystem_holdings.producer import HoldingsProducer
from subsystem_holdings.reader import FakeHoldingsMartReader, build_default_fake_reader


def build_default_offline_producer() -> HoldingsProducer:
    return HoldingsProducer(
        reader=build_default_fake_reader(),
        aligner=build_default_aligner(),
    )


def build_mock_submit_client():
    from subsystem_sdk.backends import MockSubmitBackend
    from subsystem_sdk.submit import SubmitClient

    return SubmitClient(MockSubmitBackend())


__all__ = [
    "EntityAligner",
    "EntityAlignmentResolver",
    "EntityAlignmentTable",
    "EntityRegistryAdapter",
    "AdapterDiagnostic",
    "FakeHoldingsMartReader",
    "HoldingsProducer",
    "ReadOnlyMartAdapter",
    "build_default_aligner",
    "build_default_fake_reader",
    "build_default_offline_producer",
    "build_mock_submit_client",
]
