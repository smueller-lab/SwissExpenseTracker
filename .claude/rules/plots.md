---
name: plots
description: How every Plotly chart in this project must look — theme, axes, tick alignment, height computation, chart-type conventions, and color palette.
metadata:
  type: rules
---

# Plot Design Rules

All figure code lives in `app/vis/figure.py`, `app/vis/figure_investing.py`. All rules below are mandatory for every new or modified figure function.

## Theme & base template

- Template name: `"myTemp"` — set `pio.templates.default = "myTemp"` at module level in every figure module.
- Import side-effect: `import swiss_exp_tracker.app.vis.ploty_template` registers the template; include it.
- Background: `paper_bgcolor` and `plot_bgcolor` = `"#16314d"` (must match `--color-bg-card` so charts sit seamlessly inside their cards) — defined in template, do not override.
- Font: `Segoe UI, Arial, sans-serif` (matches the CSS UI font), off-white `#e6ecf5`, size 14; title size 20 — defined in template.
- Margin defaults: `l=40, r=40, t=20, b=40` — from template via `self.vk_Margin`.
- Both axes: `showline=True`, `linecolor="white"`, `mirror=True`, `ticks="inside"`, `ticklen=5` — defined in template; set `showline=True` explicitly in `update_layout` to activate the axis border.

## Bar charts

- Always `barmode="stack"` for multi-category bars.
- Stack order: sort categories by total descending so the dominant category sits at the bottom of the stack.
- Y-axis: `dtick` via `get_adaptive_dTick(series.max())` (never a fixed `config.py` dtick — that makes the plot grow tall with the data), `range` via `get_ryAxis(dTick, series, zero_start=True)`, `showline=True`.
- Height: **fixed**, via `get_heightFigure(ry_Axis, dTick, npixel, margin)` — it returns `PLOT_TARGET_STEPS * npixel + margins` regardless of the data, so larger values raise the tick labels, not the figure height. Never hardcode a height and never let it scale with spend.
- Color: map from `vis.vk_*_col`; fallback `"#95A5A6"` for unknown categories.

## Y-axis range alignment (all chart types)

- The range end **must land on an exact tick boundary**. If `dTick=20` and data max is 115 the range ends at 120, never 125.
- Always use `get_ryAxis(dTick, series)` — it enforces this via ceiling division. Never set `yaxis.range` from raw `.max()`.
- When a range is fixed manually (e.g. `[0, 100]`), verify the endpoint is a multiple of `dtick`.
- Wrong: `range=[0, series.max() + 10]`. Right: `range=get_ryAxis(dTick, series, zero_start=True)`.

## Monthly x-axis (bar / line charts with period strings)

- Convert ISO period strings (`"2026-01-01"`) to display labels with `pd.to_datetime(p).strftime("%b %Y")` (e.g. `"Jan 2026"`).
- Sort periods lexicographically before mapping — ISO order == chronological order.
- Set `xaxis={"categoryorder": "array", "categoryarray": [labels in order]}` — this centers ticks under each bar.
- Do **not** use `type: "date"` for period strings; use `type: "category"` with explicit array order.

## Yearly x-axis (bar charts)

- `xaxis={"type": "category", "categoryorder": "array", "categoryarray": sorted_years}`.

## Date x-axis (scatter / time series with real datetime values)

- Use `get_rxAxis_Date(date_series)` → returns `(tick_vals, tick_texts, format)`.
- Tick values at every month start (`1MS` frequency), formatted `"%b %y"`.
- X range: `[min_date - pd.Timedelta(days=3), last_tick_val]`.

## Donut charts

- `hole=0.4` always.
- `pull=[0.02] * n` — uniform slight separation on all slices.
- `domain={"x": [0.0, 0.9], "y": [0.0, 1.0]}` — reserves right margin for labels.
- `textinfo="text"` with custom text: show `"label\nX.X%"` only when pct ≥ `min_pct` (default 1.0 %), else `""`.
- `textfont.size`: 11–12.
- `showlegend=False`.
- Hover: `"%{label}<br>%{value:,.2f} CHF (%{percent})<extra></extra>"`.

## Category grouping (Top-N + Other)

Charts that can show many categories (multi-category stacked bars, donuts) must fold the long tail into a single **"Other"** bucket so legends don't clip and donuts stay readable.

- Use `_collapse_top_n` (row-level relabel, for bars) or `_aggregate_top_n` (one row per category, for donuts) from `figure.py`. Both keep the top-N categories by total spend and relabel the rest to `config.category_other_label` (`"Other"`).
- Cutoffs differ by chart type: stacked bars keep `config.category_top_n_bar` (15, wider legends fit); donuts keep `config.category_top_n` (10). Pass `n=cfg.category_top_n_bar` in bar functions.
- "Other" is always the fallback grey (`vis.fallback_col`) and is ordered last via `_order_with_other_last` (top of a stack / last donut slice), regardless of its total.
- Never hand-roll the tail-grouping inline — reuse the helpers so every chart groups identically.

## Scatter / line charts

- `mode="lines"` or `"lines+markers"` — marker size 8–10.
- Line width: 2.
- Primary color: `"#19D3F3"` (cyan).
- Area fill: `fill="tonexty"`, `fillcolor="rgba(25, 211, 243, 0.15)"`.
- Reference / zero line: `add_hline(..., line={"color": "rgba(255,255,255,0.3)", "dash": "dot", "width": 1})`.
- Legend when shown: set it in Python as `{"orientation": "v", "yanchor": "middle", "y": 0.5, "xanchor": "left", "x": 1.02}` (vertical, right of the plot, vertically centered). Centered rather than top-anchored because tall/full-width charts (e.g. long monthly time series) with only a few legend entries otherwise strand the legend at the top with a large empty gutter below it. This must match `assets/mobile_legend.js`'s desktop default exactly — the JS only repositions the legend below the ≤1024px breakpoint (same breakpoint `style.css` uses to stack cards to full width — below it a card has no spare side width for a legend without crushing the plot), moving it horizontal-below the plot; above that breakpoint it's already correct on first paint, so there's no client-side reposition and no flash. Top-of-plot is *not* a supported position — don't add a width- or item-count-based fallback to it; that was tried and was fragile (races, flicker, inconsistent behavior across pages). If a legend has too many entries to fit vertically, Plotly's own scrollbar handles it.
- Plotly has no `legend.automargin` — a legend that wraps to multiple rows/columns (many categories, or a tall vertical list) does not push the plot area out of the way on its own. `assets/mobile_legend.js` handles this globally by measuring the rendered legend's actual size and setting `margin.b` or `margin.r` to fit; don't hand-roll margin for legend clearance in figure code.

## Heatmaps

- Monthly financial heatmaps: `colorscale="RdYlGn_r"` (red = expensive, green = cheap).
- Category spend heatmaps: `colorscale="Greens"`.
- Correlation heatmaps: `colorscale=[[0.0,"#b2182b"],[0.5,"#f7f7f7"],[1.0,"#2166ac"]]`, `zmin=-1`, `zmax=1`.
- Y-axis: `autorange="reversed"` for time-indexed heatmaps (newest row at top).
- Cell annotations: show CHF value as integer string; suppress zeros with `""`.
- Height: `max(220, n_rows * 35 + 80)`.

## Color palette

```python
PRIMARY  = "#19D3F3"
CYCLING  = ["#19D3F3","#E38A04","#2ECC71","#9B59B6","#E74C3C",
             "#F39C12","#1ABC9C","#E67E22","#45C7F6","#FAF263"]
FALLBACK = "#95A5A6"
```

- Colors for known merchants / categories live in `app/config.py` (`VIS` class). Never inline hex strings that duplicate what is already in `VIS`.
- New color maps go in `VIS` in `config.py`; new dtick / npixel values go in `config` dataclass.

## Utility functions — always use, never reimplement

| Function | Returns | Use for |
|----------|---------|---------|
| `get_ryAxis(dTick, series, zero_start)` | `[y_min, y_max]` snapped to dTick | every y-axis range |
| `get_rxAxis_Date(date_series)` | `(tick_vals, tick_texts, format)` | datetime x-axis |
| `get_heightFigure(ry_axis, dTick, npixel, margin)` | fixed height in px (`PLOT_TARGET_STEPS * npixel + margins`) | every bar/scatter figure — height never scales with the data |
| `get_adaptive_dTick(max_value, target_steps=8)` | dTick float | the y-axis dtick for every value-axis chart, so ticks adapt to the data |
