from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pandas as pd


def get_ryAxis(
    d_Tick: float,
    z: pd.Series,
    q_ZeroStart: bool = False,
) -> list[float]:
    y_start: float = 0 if q_ZeroStart else z.min() // d_Tick * d_Tick
    y_end: float = ((z.max() + d_Tick - 1) // d_Tick) * d_Tick
    return [y_start, y_end]


def get_rxAxis_Date(z_Date: pd.Series) -> tuple[Any, list[str], str]:
    format_Date = "%b %y"
    z_Date = pd.to_datetime(z_Date)

    Date_start = z_Date.min().replace(day=1)
    Date_end = z_Date.max()
    Date_end_next = (Date_end + pd.offsets.MonthBegin(1)).normalize()

    s_tick_val = pd.date_range(start=Date_start, end=Date_end_next, freq="1MS")

    s_tick_text = [Date.strftime(format_Date) for Date in s_tick_val]

    return s_tick_val, s_tick_text, format_Date


def get_heightFigure(
    ry_Axis: Sequence[float],
    dTick: float,
    npixel: float,
    vk_Margin: dict[str, Any],
) -> float:
    ymin, ymax = ry_Axis
    n_step = int((ymax - ymin) / dTick)
    h_plot = n_step * npixel
    return float(h_plot + vk_Margin["t"] + vk_Margin["b"])
