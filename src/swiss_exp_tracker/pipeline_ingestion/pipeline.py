from __future__ import annotations

import importlib
import logging
import sys

from pathlib import Path
from typing import Any


if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from swiss_exp_tracker.pipeline_ingestion.db import create_all_tables


logger = logging.getLogger(__name__)

# Numeric-prefixed modules must be loaded via importlib
_mod_landing = importlib.import_module(
    "swiss_exp_tracker.pipeline_ingestion.stages.stage_01_landing"
)
_mod_raw = importlib.import_module(
    "swiss_exp_tracker.pipeline_ingestion.stages.stage_02_raw"
)
_mod_refined = importlib.import_module(
    "swiss_exp_tracker.pipeline_ingestion.stages.stage_03_refined"
)
_mod_postprocess = importlib.import_module(
    "swiss_exp_tracker.pipeline_ingestion.stages.stage_04_postprocess"
)

run_landing = _mod_landing.run_landing
run_raw = _mod_raw.run_raw
run_refined = _mod_refined.run_refined
run_postprocess = _mod_postprocess.run_postprocess


def run_ingestion() -> dict[str, Any]:
    """Run landing → raw → refined → postprocess in sequence and return stage-level stats."""
    create_all_tables()

    logger.info("[pipeline] Stage 1: landing")
    landing_result = run_landing()
    logger.info("           %s", landing_result)

    logger.info("[pipeline] Stage 2: raw")
    raw_result = run_raw()
    logger.info("           %s", raw_result)

    logger.info("[pipeline] Stage 3: refined")
    refined_result = run_refined()
    logger.info("           %s", refined_result)

    logger.info("[pipeline] Stage 4: postprocess")
    postprocess_result = run_postprocess()
    logger.info("           %s", postprocess_result)

    return {
        "landing": landing_result,
        "raw": raw_result,
        "refined": refined_result,
        "postprocess": postprocess_result,
    }


def run() -> None:
    """Full pipeline: ingestion (landing → raw → refined → postprocess) then agentic enrichment."""
    run_ingestion()


if __name__ == "__main__":
    run()
