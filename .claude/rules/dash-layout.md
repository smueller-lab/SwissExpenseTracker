---
name: dash-layout
description: Dash component, callback, grid spacing, and card width conventions for every dashboard page in this project.
metadata:
  type: rules
---

# Dash Layout Rules

## Page structure

- Layout files live in `app/layout/<page>.py` and export a single `layout()` function returning `html.Div`.
- Callbacks live in `app/callbacks/<page>.py` and are imported and registered in `app/app.py`.
- `dcc.Graph` always has `figure={}` as its static default; the real figure is injected by a callback. Never compute a figure inside a layout function.
- `dcc.Dropdown` options follow `[{"label": x, "value": x}]` built from data — never hardcoded lists.
- KPI cards use `components/cards.py` helpers (`make_number_card`, `make_figure_card`, etc.). Never write inline `html.Div` card markup in a layout file.
- Graphs that query the DB must be wrapped in `dcc.Loading`.

## No inline styles — ever

Never use the `style={}` prop on any Dash component. All visual styling (colors, spacing, font sizes, borders, flex layout, etc.) lives exclusively in the CSS stylesheet. Components reference styles only via `className=`.

- Wrong: `html.Div(style={"marginTop": "12px", "color": "#fff"})`
- Right: `html.Div(className="card-header")`

This applies to spacing corrections too — if a card looks misaligned, fix the CSS class, not the component.

## Grid, spacing, and card widths

The layout uses a 12-column grid (`className="grid"`). Every card declares its column span via the `width` argument of a `make_*_card` helper, which renders as `className=f"card col-{width}"`.

**All cards on a page live inside a single `className="grid"` div.** Never split rows into separate `className="grid"` containers. The CSS grid's `gap: var(--card-gap)` applies between every item in the grid — both within a row and between rows — giving consistent spacing throughout. Multiple stacked grid containers produce zero spacing between rows.

- Wrong: three separate `html.Div(..., className="grid")` elements for three rows.
- Right: one `html.Div([card1, card2, ..., cardN], className="grid")` containing all cards; the grid wraps naturally when column spans reach 12.

## Category color consistency

When the same set of categories appears in more than one chart on the same page (e.g. a bar chart and a donut, or a boxplot and a donut), every chart must assign the same color to the same category. A category that is red in one chart must not be blue in another on the same page.

**How to achieve this:** Plotly assigns colors from the template colorway by trace/slice position (colorway[0] for the first trace, colorway[1] for the second, etc.). To keep colors consistent, sort the categories in the same order — typically total spend descending — before building every figure that shows those categories. Do not mix sort keys (e.g. median in one chart, total in another) when color consistency matters.

- Do not hard-code a single fallback color (e.g. `"#95A5A6"`) for all traces in a multi-category figure — this collapses all categories to grey and loses the distinction.
- When a figure uses explicit `marker_color`, build the color list from the same sorted category order used by the other charts on the page.
- When categories have no named color map in `VIS`, omit `marker_color` entirely and let Plotly cycle through its colorway automatically.

**Row totals must equal 12.** Sum the `width` values of all cards in a logical row:
- Sum < 12: dead space — widen a card or add a card.
- Sum > 12: cards overlap — reduce a width or split to a new row.
Both are layout bugs and must be fixed.

**Minimum widths for plot cards.** Never use `width ≤ 3` for any `dcc.Graph`. At `col-3` or narrower, x-axis tick labels, y-axis labels, and legends are cut off or squished.
- Minimum safe: `col-4`.
- Preferred for monthly-label charts or charts with a legend: `col-5` or `col-6`.

**Width defaults by card type** (adjust only when content genuinely benefits):

| Card type | Default width | Notes |
|-----------|--------------|-------|
| `make_number_card` (KPI) | `col-3` | 4 per row |
| `make_figure_card` (chart) | `col-6` | side-by-side pair |
| Full-width time series / heatmap | `col-8` or `col-12` | |
| Donut chart | `col-5` preferred, `col-4` minimum | wider when labels are dense |
| `make_table_card` alongside a chart | `col-4` to `col-5` | |

When in doubt, choose wider rather than narrower — breathing room beats clipped labels.

## Interactive element colors

All colors for buttons, dropdowns, checkboxes, and scrollbars must use CSS variables defined in `app/assets/style.css`. Never write raw hex or rgba values in CSS rules — add a token to `:root` first.

### Canonical tokens for interactive elements

| Element | State | CSS variable |
|---------|-------|-------------|
| Dropdown control box background | — | `var(--color-bg-dropdown)` |
| Dropdown menu/list background | — | `var(--color-bg-dropdown-menu)` |
| Dropdown control border | — | `var(--color-accent-border)` |
| Dropdown option | hover / focused | `var(--color-accent-soft)` |
| Dropdown option | selected | `var(--color-accent-selected)` |
| Dropdown option text | selected | `var(--color-text-accent)` |
| Button background | default | `var(--color-bg-btn)` |
| Button background | hover | `var(--color-bg-btn-hover)` |
| Button background | active / pressed | `var(--color-bg-btn-active)` |
| Button border | default | `var(--color-border-btn)` |
| Button border | active | `var(--color-border-btn-active)` |
| Button text | all states | `var(--color-white)` |
| Checkbox tick | — | `accent-color: var(--color-accent)` |
| Scrollbar thumb | — | `var(--color-scrollbar-thumb)` |
| Scrollbar track | — | `var(--color-scrollbar-track)` |

### Rules

- Both `dropdown-year` and `smart-filter-bar` dropdowns must use the same token for each state. Any new dropdown component follows the same table.
- `--color-bg-dropdown` (`#295886`) is for the closed control box (the visible "pill"). `--color-bg-dropdown-menu` (`#1e2d45`) is for the open menu list — darker to create depth.
- `--color-accent-border` (`rgba(102, 178, 255, 0.25)`) is the standard border color for all interactive controls (dropdown control, menu outer).
- Never use `#ffffff` directly — use `var(--color-white)`.
- When adding a new shade of the accent color, add a named token to `:root` and use it everywhere that shade appears.
