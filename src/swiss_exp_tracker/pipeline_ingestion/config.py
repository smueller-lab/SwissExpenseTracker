from __future__ import annotations

from pathlib import Path

from user_config import DIR_BOX


PROJECT_ROOT = Path(__file__).resolve().parents[3]

INGESTION_DB_DIR = PROJECT_ROOT / "database"
INGESTION_DB_PATH = INGESTION_DB_DIR / "transactions.db"
POSITIONS_DB_PATH = INGESTION_DB_DIR / "positions.db"
LANDING_ZONE_DIR = DIR_BOX / "lnd"


def init_paths() -> None:
    INGESTION_DB_DIR.mkdir(parents=True, exist_ok=True)
    LANDING_ZONE_DIR.mkdir(parents=True, exist_ok=True)


init_paths()
