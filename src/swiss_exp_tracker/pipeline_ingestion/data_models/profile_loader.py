from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

from swiss_exp_tracker.pipeline_ingestion.data_models.source_profile import (
    SourceProfile,
)
from swiss_exp_tracker.pipeline_ingestion.data_models.source_type import SourceType

_PROFILES_PATH = Path(__file__).parent / "source_profiles.yaml"


@lru_cache(maxsize=1)
def load_profiles() -> dict[SourceType, SourceProfile]:
    """Read source_profiles.yaml next to this module and return a validated SourceType-keyed map.

    Cached after the first call so repeated invocations do not re-read the file.
    """
    with _PROFILES_PATH.open(encoding="utf-8") as fh:
        raw: dict[str, Any] = yaml.safe_load(fh)
    return {
        SourceType(key): SourceProfile.model_validate({"source": key, **block})
        for key, block in raw.items()
    }
