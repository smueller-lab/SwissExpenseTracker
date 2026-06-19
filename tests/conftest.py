"""Pytest configuration to add src directory to Python path."""

from __future__ import annotations

import os
import sys
import tempfile

from pathlib import Path

# Provide a throwaway DATA_DIR so user_config does not raise during test
# collection. Tests redirect the real DB/landing paths via monkeypatch, so the
# value only needs to exist — it is never read for real data.
os.environ.setdefault("DATA_DIR", tempfile.mkdtemp(prefix="set_test_data_"))

# Add src to path so swiss_exp_tracker imports work
src_path = Path(__file__).resolve().parent.parent / "src"
sys.path.insert(0, str(src_path))
