from __future__ import annotations

import json
import math
import re

from datetime import date
from datetime import datetime
from pathlib import Path

import pandas as pd

from swiss_exp_tracker.db.sql import positions
from swiss_exp_tracker.pipeline_ingestion.config import LANDING_ZONE_DIR
from swiss_exp_tracker.pipeline_ingestion.data_models.position import (
    SwissquotePositionRaw,
)
from swiss_exp_tracker.pipeline_ingestion.data_models.source_type import SourceType
from swiss_exp_tracker.pipeline_ingestion.db_positions import create_positions_tables
from swiss_exp_tracker.pipeline_ingestion.db_positions import get_positions_connection
from swiss_exp_tracker.pipeline_ingestion.file_tracker_positions import (
    get_latest_positions_file_id,
)
from swiss_exp_tracker.pipeline_ingestion.file_tracker_positions import (
    get_new_positions_files,
)
from swiss_exp_tracker.pipeline_ingestion.file_tracker_positions import (
    mark_positions_file_processed,
)

_FILENAME_RE = re.compile(r"Positions_(\d+)_(\d{2})(\d{2})(\d{4})_\d{2}_\d{2}\.xls")


def _cell_str(value: object) -> str:
    """Convert a cell value to a stripped string, returning '' for None or NaN."""
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    return str(value).strip()


def _is_numeric(value: object) -> bool:
    """Return True if value is a non-NaN number (int or float)."""
    if isinstance(value, int):
        return True
    if isinstance(value, float):
        return not math.isnan(value)
    return False


def _parse_filename(filename: str) -> tuple[str, str]:
    """Return (account_id, snapshot_date_iso) from a Swissquote positions filename."""
    m = _FILENAME_RE.match(filename)
    if m is None:
        msg = f"Cannot parse Swissquote filename: {filename!r}"
        raise ValueError(msg)
    account_id = m.group(1)
    day, month, year = int(m.group(2)), int(m.group(3)), int(m.group(4))
    return account_id, date(year, month, day).isoformat()


def _parse_positions_xls(file_path: Path) -> list[SwissquotePositionRaw]:
    """Parse a Swissquote XLS positions file into a list of SwissquotePositionRaw models."""
    account_id, snapshot_date = _parse_filename(file_path.name)

    frame = pd.read_excel(file_path, engine="xlrd", header=None)
    parsed_positions: list[SwissquotePositionRaw] = []
    current_asset_class = ""

    for _, row in frame.iloc[1:].iterrows():
        s0 = _cell_str(row.iloc[0])
        s1 = _cell_str(row.iloc[1])

        # Asset class header row (e.g. 'Shares', 'ETFs', 'Aktien')
        if s0 and not s1:
            current_asset_class = s0
            continue

        # Position row: non-empty symbol, blank row-marker, numeric quantity.
        # Subtotal/total rows (any language) always have a non-numeric quantity.
        if s1 and not s0 and _is_numeric(row.iloc[2]):
            name = _cell_str(row.iloc[13]) if len(row) > 13 else ""
            parsed_positions.append(
                SwissquotePositionRaw(
                    symbol=s1,
                    asset_class=current_asset_class,
                    quantity=row.iloc[2],
                    unit_cost=row.iloc[3],
                    total_value=row.iloc[4],
                    # cols 5 (daily_change) and 6 (daily_change_pct) skipped
                    price=row.iloc[7],
                    currency=_cell_str(row.iloc[8]),
                    pnl_nominal_chf=row.iloc[9],
                    pnl_pct_chf=row.iloc[10],
                    total_value_chf=row.iloc[11],
                    position_weight_pct=row.iloc[12],
                    snapshot_date=snapshot_date,
                    account_id=account_id,
                    name=name or None,
                )
            )

    return parsed_positions


def run_positions_landing() -> dict[str, int]:
    """Process new Swissquote XLS files: parse, register, and batch-insert to positions_lnd."""
    create_positions_tables()
    folder = LANDING_ZONE_DIR / SourceType.SWISSQUOTE.value.lower()
    new_files = get_new_positions_files(folder)

    files_processed = 0
    records_inserted = 0

    for file_path in new_files:
        rows = _parse_positions_xls(file_path)
        if not rows:
            continue

        mark_positions_file_processed(file_path, len(rows), "landing")
        file_id = get_latest_positions_file_id(file_path.name)

        now = datetime.now().isoformat()
        rows_data = [
            {
                "file_id": file_id,
                "raw_json": json.dumps(row.model_dump(mode="json")),
                "created_at": now,
            }
            for row in rows
        ]
        with get_positions_connection() as db:
            positions.insert_positions_lnd(db, rows_data)

        records_inserted += len(rows)
        files_processed += 1

    return {
        "files_found": len(new_files),
        "files_processed": files_processed,
        "records_inserted": records_inserted,
    }
