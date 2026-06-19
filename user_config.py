from __future__ import annotations

import os

from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

_raw = os.getenv("DATA_DIR")
if not _raw:
    raise RuntimeError(
        "DATA_DIR is not set — add it to your .env file pointing to the folder that contains your lnd/ directory."
    )

DIR_DATA = Path(_raw)
