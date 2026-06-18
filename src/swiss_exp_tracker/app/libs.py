from __future__ import annotations

import math

from typing import Any

import pandas as pd

# Fixed gridline count for every value-axis plot. Height is pinned to this many
# steps so a plot never grows tall with the data; get_adaptive_dTick raises the
# tick values instead. Data still fills the plot because the y-range maps onto
# the full (fixed) pixel height.
PLOT_TARGET_STEPS = 8


def get_adaptive_dTick(
    max_value: float, target_steps: int = PLOT_TARGET_STEPS
) -> float:
    """Return a round dTick so max_value fits in ~target_steps steps.

    Keeps the step count bounded regardless of data scale, so a fixed-height plot
    shows readable, evenly spaced tick values at any spending level.
    """
    if max_value <= 0:
        return 1.0
    raw = max_value / target_steps
    magnitude: float = 10 ** math.floor(math.log10(raw))
    for multiplier in [1, 2, 5, 10]:
        candidate: float = multiplier * magnitude
        if max_value / candidate <= target_steps:
            return candidate
    return magnitude * 10


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
    npixel: float,
    vk_Margin: dict[str, Any],
) -> float:
    """Return a fixed plot height (PLOT_TARGET_STEPS x npixel + margins) that does not grow with spending."""
    h_plot = PLOT_TARGET_STEPS * npixel
    return float(h_plot + vk_Margin["t"] + vk_Margin["b"])
