---
name: new-page
description: Scaffolds a new dashboard page end-to-end — layout, callbacks, app registration, and stub data loader method.
---

Scaffold a new dashboard page for SwissExpenseTracker. The argument passed is the page name in snake_case (e.g. `retail`, `transport`, `m_cumulus`).

## What to create

Given page name `<page>`:

### 1. `src/swiss_exp_tracker/app/layout/<page>.py`

```python
from __future__ import annotations

from typing import Any

from dash import html

from swiss_exp_tracker.app.components.cards import make_figure_card
from swiss_exp_tracker.app.components.cards import make_number_card


def layout(data: Any) -> Any:
    return html.Div(
        [
            # TODO: add cards here
            # Example: make_number_card("Total", data.z_<page>_total)
            # Example: make_figure_card("<Chart Title>", fig_id="fig-<page>-main", width=8)
        ],
        className="grid",
    )
```

Rules:
- All cards go inside the `className="grid"` div.
- No `style={}` props — use `className=` only.
- Width values in each row must sum to 12.
- `dcc.Graph` must use `figure={}` default (injected by callback); import `make_figure_card_MonthYear` if the chart needs a Monthly/Yearly toggle.

### 2. `src/swiss_exp_tracker/app/callbacks/<page>.py`

```python
from __future__ import annotations

from dash import Input
from dash import Output
from dash import callback

from swiss_exp_tracker.app.data.loader import DataLoader


def register_callbacks(loader: DataLoader) -> None:
    @callback(  # type: ignore[untyped-decorator]
        Output("fig-<page>-main", "figure"),
        Input("fig-<page>-main", "id"),
    )
    def update_<page>_main(_: str) -> object:
        # TODO: build figure from loader data
        return {}
```

### 3. Register in `app/app.py`

Add the import and register call alongside the existing pages. Find the block where other callbacks are registered and add:
```python
from swiss_exp_tracker.app.callbacks import <page> as cb_<page>
cb_<page>.register_callbacks(loader)
```

Add the layout route inside the page routing callback.

### 4. Add stub to `app/data/loader.py`

Add a stub attribute or method for the data this page will need. Follow the existing `DataLoader` pattern — load from SQLite in `__init__` or as a method, store as a `pd.DataFrame`.

### 5. Remind the user

After scaffolding, remind the user of what still needs to be done:
- Populate the `layout()` function with real cards (widths summing to 12).
- Implement the callback functions using the plotting agent for figure code.
- Add the page link to the navigation if a nav component exists.
- Write tests for any new data loader method (hand off to the testing agent).
