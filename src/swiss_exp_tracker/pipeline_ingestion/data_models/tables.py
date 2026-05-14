from __future__ import annotations

from pydantic import BaseModel

from swiss_exp_tracker.pipeline_ingestion.data_models.source_type import SourceType


class LandingRow(BaseModel):
    landing_id: int
    file_id: int
    source_type: SourceType
    raw_json: str
    source_file: str


class RawRow(BaseModel):
    raw_id: int
    landing_id: int
    file_id: int
    source_type: SourceType
    raw_json: str
    source_file: str
