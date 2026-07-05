from __future__ import annotations

from swiss_exp_tracker.pipeline_ingestion.data_models.profile_loader import (
    load_profiles,
)
from swiss_exp_tracker.pipeline_ingestion.data_models.source_profile import (
    SourceProfile,
)
from swiss_exp_tracker.pipeline_ingestion.data_models.source_type import SourceType

SUPPORTED_SOURCES: set[SourceType] = set(load_profiles().keys())


def get_profile(source: SourceType) -> SourceProfile:
    """Return the SourceProfile for source; raise NotImplementedError if unregistered."""
    profiles = load_profiles()
    if source not in profiles:
        raise NotImplementedError(
            f"No SourceProfile registered for source type '{source.value}'. "
            f"Registered sources: {[s.value for s in profiles]}"
        )
    return profiles[source]
