import pandas as pd

def get_ryAxis(d_Tick: int, z: pd.Series, q_ZeroStart: bool = False):
    if q_ZeroStart:
        y_start = 0
    else:
        y_start = (z.min() // d_Tick) * d_Tick
    
    y_end = ((z.max() + d_Tick - 1) // d_Tick) * d_Tick
    ry_Axes = [y_start, y_end]
    return ry_Axes

def get_rxAxis_Date(z_Date: pd.Series):
    format_Date = '%b %y'
    z_Date = pd.to_datetime(z_Date)

    Date_start = z_Date.min().replace(day=1)
    Date_end = z_Date.max()
    Date_end_next = (Date_end + pd.offsets.MonthBegin(1)).normalize()

    s_tick_val = pd.date_range(
        start=Date_start,
        end=Date_end_next,
        freq='1MS'
    )

    s_tick_text = [Date.strftime(format_Date) for Date in s_tick_val]

    return s_tick_val, s_tick_text, format_Date