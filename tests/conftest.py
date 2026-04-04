"""Pytest configuration to add src directory to Python path."""

from __future__ import annotations

import sys

from pathlib import Path


# Add src to path so swiss_exp_tracker imports work
src_path = Path(__file__).resolve().parent.parent / "src"
sys.path.insert(0, str(src_path))
