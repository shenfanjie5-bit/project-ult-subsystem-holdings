from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from subsystem_sdk.backends.config import SubmitBackendConfig
from subsystem_sdk.backends.data_platform_queue import (
    SubmitCandidateFunc,
)
from subsystem_sdk.backends.factory import build_submit_backend
from subsystem_sdk.submit import SubmitClient
from subsystem_sdk.validate.entity_registry import EntityPreflightProfile
from subsystem_sdk.validate.preflight import EntityRegistryLookup, PreflightPolicy

SubmitCandidateIdempotentFunc = Callable[[Mapping[str, Any]], Any]


def build_data_platform_queue_submit_client(
    submit_candidate_func: SubmitCandidateFunc | None = None,
    submit_candidate_idempotent_func: SubmitCandidateIdempotentFunc | None = None,
    *,
    entity_lookup: EntityRegistryLookup | None = None,
    preflight_policy: PreflightPolicy = "skip",
    entity_preflight_profile: EntityPreflightProfile = "dev",
    idempotent_required: bool = False,
) -> SubmitClient:
    config_kwargs: dict[str, Any] = {"backend_kind": "data_platform_queue"}
    if idempotent_required:
        config_kwargs["data_platform_idempotent_required"] = True
    try:
        config = SubmitBackendConfig(**config_kwargs)
    except (TypeError, ValueError) as exc:
        if idempotent_required:
            raise RuntimeError(
                "data_platform_queue idempotent submit requires subsystem-sdk "
                "data_platform_idempotent_required support"
            ) from exc
        raise

    backend_kwargs: dict[str, Any] = {
        "data_platform_submit_candidate": submit_candidate_func,
    }
    if submit_candidate_idempotent_func is not None:
        backend_kwargs[
            "data_platform_submit_candidate_idempotent"
        ] = submit_candidate_idempotent_func
    try:
        backend = build_submit_backend(config, **backend_kwargs)
    except TypeError as exc:
        if idempotent_required or submit_candidate_idempotent_func is not None:
            raise RuntimeError(
                "data_platform_queue idempotent submit requires subsystem-sdk "
                "idempotent backend factory support"
            ) from exc
        raise

    return SubmitClient(
        backend,
        entity_lookup=entity_lookup,
        preflight_policy=preflight_policy,
        entity_preflight_profile=entity_preflight_profile,
    )


__all__ = ["build_data_platform_queue_submit_client"]
