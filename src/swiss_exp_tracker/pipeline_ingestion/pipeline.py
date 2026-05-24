from __future__ import annotations

import logging
import sys

from pathlib import Path
from typing import Any


if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from swiss_exp_tracker.pipeline_ingestion.db import create_all_tables
from swiss_exp_tracker.pipeline_ingestion.db_positions import create_positions_tables
from swiss_exp_tracker.pipeline_ingestion.stages.positions.stage_01_landing import (
    run_positions_landing,
)
from swiss_exp_tracker.pipeline_ingestion.stages.positions.stage_02_raw import (
    run_positions_raw,
)
from swiss_exp_tracker.pipeline_ingestion.stages.positions.stage_03_rfn import (
    run_positions_rfn,
)
from swiss_exp_tracker.pipeline_ingestion.stages.positions.stage_04_use import (
    run_positions_use,
)
from swiss_exp_tracker.pipeline_ingestion.stages.transactions.stage_01_landing import (
    run_landing,
)
from swiss_exp_tracker.pipeline_ingestion.stages.transactions.stage_02_raw import (
    run_raw,
)
from swiss_exp_tracker.pipeline_ingestion.stages.transactions.stage_03_refined import (
    run_refined,
)
from swiss_exp_tracker.pipeline_ingestion.stages.transactions.stage_04_postprocess import (
    run_postprocess,
)


logger = logging.getLogger(__name__)


def run_positions_pipeline() -> dict[str, Any]:
    """Run the Swissquote positions pipeline: landing → raw → refined → use."""
    create_positions_tables()

    logger.info("[positions] Stage 1: landing")
    landing_result = run_positions_landing()

    logger.info("[positions] Stage 2: raw")
    raw_result = run_positions_raw()

    logger.info("[positions] Stage 3: refined")
    rfn_result = run_positions_rfn()

    logger.info("[positions] Stage 4: use")
    use_result = run_positions_use()

    return {
        "landing": landing_result,
        "raw": raw_result,
        "refined": rfn_result,
        "use": use_result,
    }


def run_ingestion() -> dict[str, Any]:
    """Run landing → raw → refined → postprocess in sequence and return stage-level stats."""
    create_all_tables()

    logger.info("[pipeline] Stage 1: landing")
    landing_result = run_landing()

    logger.info("[pipeline] Stage 2: raw")
    raw_result = run_raw()

    logger.info("[pipeline] Stage 3: refined")
    refined_result = run_refined()

    logger.info("[pipeline] Stage 4: postprocess")
    postprocess_result = run_postprocess()

    logger.info("[pipeline] Stage 5: positions")
    positions_result = run_positions_pipeline()

    return {
        "landing": landing_result,
        "raw": raw_result,
        "refined": refined_result,
        "postprocess": postprocess_result,
        "positions": positions_result,
    }


def run() -> None:
    """Full pipeline: ingestion (landing → raw → refined → postprocess) then agentic enrichment."""
    run_ingestion()


if __name__ == "__main__":
    run()
