from __future__ import annotations

from subsystem_sdk.backends.data_platform_queue import (
    DataPlatformQueueSubmitBackend,
    SubmitCandidateFunc,
)
from subsystem_sdk.submit import SubmitClient
from subsystem_sdk.validate.entity_registry import EntityPreflightProfile
from subsystem_sdk.validate.preflight import EntityRegistryLookup, PreflightPolicy


def build_data_platform_queue_submit_client(
    submit_candidate_func: SubmitCandidateFunc | None = None,
    *,
    entity_lookup: EntityRegistryLookup | None = None,
    preflight_policy: PreflightPolicy = "skip",
    entity_preflight_profile: EntityPreflightProfile = "dev",
) -> SubmitClient:
    return SubmitClient(
        DataPlatformQueueSubmitBackend(
            submit_candidate_func=submit_candidate_func,
        ),
        entity_lookup=entity_lookup,
        preflight_policy=preflight_policy,
        entity_preflight_profile=entity_preflight_profile,
    )


__all__ = ["build_data_platform_queue_submit_client"]
