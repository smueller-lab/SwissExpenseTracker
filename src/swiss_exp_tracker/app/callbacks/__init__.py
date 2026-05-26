from __future__ import annotations

from typing import Any

from swiss_exp_tracker.app.callbacks import food
from swiss_exp_tracker.app.callbacks import groceries
from swiss_exp_tracker.app.callbacks import groceries_detail
from swiss_exp_tracker.app.callbacks import home
from swiss_exp_tracker.app.callbacks import investing
from swiss_exp_tracker.app.callbacks import retail
from swiss_exp_tracker.app.callbacks import router
from swiss_exp_tracker.app.callbacks import smarttable
from swiss_exp_tracker.app.callbacks import sport
from swiss_exp_tracker.app.callbacks import transport


_PAGE_CALLBACKS = [
    home,
    food,
    groceries,
    groceries_detail,
    sport,
    transport,
    retail,
    smarttable,
]


def register_all_callbacks(app: Any, data: Any, pos: Any) -> None:
    router.register_callbacks(app, data, pos)
    for module in _PAGE_CALLBACKS:
        module.register_callbacks(app, data)
    investing.register_callbacks(app, data, pos)
