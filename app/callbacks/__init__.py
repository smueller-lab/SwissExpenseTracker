from __future__ import annotations

from typing import Any

from app.callbacks import food
from app.callbacks import groceries
from app.callbacks import home
from app.callbacks import router
from app.callbacks import sport


S_CALLBACKS = [home, router, food, groceries, sport]


def register_all_callbacks(app: Any, data: Any) -> None:
    for module in S_CALLBACKS:
        module.register_callbacks(app, data)
