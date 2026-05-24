from __future__ import annotations

from typing import Any

from swiss_exp_tracker.app.callbacks import food
from swiss_exp_tracker.app.callbacks import groceries
from swiss_exp_tracker.app.callbacks import home
from swiss_exp_tracker.app.callbacks import retail
from swiss_exp_tracker.app.callbacks import router
from swiss_exp_tracker.app.callbacks import smarttable
from swiss_exp_tracker.app.callbacks import sport
from swiss_exp_tracker.app.callbacks import transport


S_CALLBACKS = [home, router, food, groceries, sport, transport, retail, smarttable]


def register_all_callbacks(app: Any, data: Any) -> None:
    for module in S_CALLBACKS:
        module.register_callbacks(app, data)
