from __future__ import annotations

from subsystem_sdk.backends import DataPlatformQueueSubmitBackend, SubmitCandidateFunc
from subsystem_sdk.submit import SubmitClient


def build_data_platform_queue_submit_client(
    submit_candidate_func: SubmitCandidateFunc | None = None,
) -> SubmitClient:
    return SubmitClient(
        DataPlatformQueueSubmitBackend(
            submit_candidate_func=submit_candidate_func,
        )
    )


__all__ = ["build_data_platform_queue_submit_client"]
