from __future__ import annotations

from typing import Any
from typing import Literal
from typing import cast

import pandas as pd
import plotly.graph_objects as go  # pyright: ignore[reportMissingTypeStubs]

from dash import ALL
from dash import Input
from dash import Output
from dash import State
from dash import ctx
from dash import no_update

from swiss_exp_tracker.app.components.cards import make_budget_rows
from swiss_exp_tracker.app.components.cards import make_budget_table_card
from swiss_exp_tracker.app.config import config
from swiss_exp_tracker.app.data import forecast
from swiss_exp_tracker.app.data.budget_models import CategoryBudget
from swiss_exp_tracker.app.layout.budget_forecast import BUDGET_TABLE_COLUMNS
from swiss_exp_tracker.app.vis.figure import Fig
from swiss_exp_tracker.app.vis.figure import get_fig_BudgetForecast

cfg = config()
_fig_helper = Fig()


def register_callbacks(app: Any, data: Any) -> None:
    """Register save-budgets and refresh-forecast callbacks on the budget/forecast page."""

    @app.callback(  # type: ignore[untyped-decorator]
        Output("budget-save-status", "children"),
        Input("budget-save-btn", "n_clicks"),
        State("budget-year", "value"),
        State({"type": "budget-input", "category": ALL}, "value"),
        State({"type": "budget-input", "category": ALL}, "id"),
        prevent_initial_call=True,
    )
    def save_budgets(  # pyright: ignore[reportUnusedFunction]
        n_clicks: int | None,
        year: int | None,
        values: list[float | None],
        ids: list[dict[str, str]],
    ) -> Any:
        """Persist per-category budgets to the DB; return a status message."""
        if not n_clicks:
            return no_update
        budgets = [
            CategoryBudget.model_validate(
                {"category": id_dict["category"], "budget_chf": val}
            )
            for val, id_dict in zip(values, ids, strict=False)
        ]
        data.save_budgets(int(year), budgets)  # type: ignore[arg-type]
        return f"✓ Saved {len(budgets)} budgets"

    @app.callback(  # type: ignore[untyped-decorator]
        Output("budget-input-rows", "children"),
        Output("budget-add-select", "options"),
        Output("budget-add-select", "value"),
        Input("budget-active-categories", "data"),
        Input("budget-year", "value"),
    )
    def render_budget_rows(  # pyright: ignore[reportUnusedFunction]
        active: list[str] | None,
        year: int | None,
    ) -> tuple[Any, list[dict[str, str]], Any]:
        """Render the per-category input rows and refresh the add-dropdown to exclude active categories."""
        active_cats = active or []
        budgets = data.get_budgets(int(year)) if year is not None else []
        lookup = {b.category: b.budget_chf for b in budgets}
        rows = make_budget_rows(active_cats, lookup)
        options = [
            {"label": c, "value": c}
            for c in data.budget_categories_all
            if c not in active_cats
        ]
        return rows, options, None

    @app.callback(  # type: ignore[untyped-decorator]
        Output("budget-active-categories", "data", allow_duplicate=True),
        Input("budget-add-btn", "n_clicks"),
        State("budget-add-select", "value"),
        State("budget-active-categories", "data"),
        prevent_initial_call=True,
    )
    def add_category(  # pyright: ignore[reportUnusedFunction]
        n_clicks: int | None,
        new_cat: str | None,
        active: list[str] | None,
    ) -> Any:
        """Append the chosen category to the active budget set when the add button is clicked."""
        active_cats = active or []
        if not n_clicks or not new_cat or new_cat in active_cats:
            return no_update
        return [*active_cats, new_cat]

    @app.callback(  # type: ignore[untyped-decorator]
        Output("budget-active-categories", "data", allow_duplicate=True),
        Input({"type": "budget-remove", "category": ALL}, "n_clicks"),
        State("budget-active-categories", "data"),
        State("budget-year", "value"),
        prevent_initial_call=True,
    )
    def remove_category(  # pyright: ignore[reportUnusedFunction]
        n_clicks: list[int | None],
        active: list[str] | None,
        year: int | None,
    ) -> Any:
        """Drop the category whose remove button was clicked and clear its saved budget from the DB."""
        triggered = ctx.triggered_id
        if not any(n_clicks) or not isinstance(triggered, dict):
            return no_update
        removed = triggered["category"]
        if year is not None:
            data.delete_budget(int(year), removed)
        return [c for c in (active or []) if c != removed]

    @app.callback(  # type: ignore[untyped-decorator]
        Output("budget-forecast-fig", "figure"),
        Output("budget-table-container", "children"),
        Input("budget-year", "value"),
        Input("budget-active-categories", "data"),
        Input("budget-save-btn", "n_clicks"),
    )
    def refresh_forecast(  # pyright: ignore[reportUnusedFunction]
        year: int | None,
        selected_cats: list[str] | None,
        _n_clicks: int | None,
    ) -> tuple[Any, Any]:
        """Rebuild the cumulative forecast chart and budget table from current selections."""
        cats = selected_cats or []
        empty_table = make_budget_table_card(
            "\U0001f4ca Budget vs Forecast by Category",
            BUDGET_TABLE_COLUMNS,
            [],
            width=12,
        )

        if not cats or year is None:
            return go.Figure(), empty_table

        year_int = int(year)
        freq_lit = cast("Literal['D', 'W', 'M']", cfg.forecast_freq_default)
        as_of = min(
            pd.Timestamp.today().normalize(),
            pd.Timestamp(year=year_int, month=12, day=31),
        )

        actual = data.get_category_cumulative(year_int, cats, freq_lit)
        history = data.get_category_period_history(cats, year_int, freq_lit)
        monthly_totals = data.get_category_monthly_totals(
            cats, as_of, cfg.forecast_lumpy_lookback_years
        )

        n_completed_this_year = as_of.month - 1

        parts: list[pd.DataFrame] = []
        curves: dict[str, pd.DataFrame] = {}
        levels: dict[str, float | None] = {}
        lumpy_rates: dict[str, float] = {}
        lumpy_deviations: dict[str, float] = {}
        spike_excess: dict[str, float] = {}
        for cat in cats:
            curve = forecast.seasonal_pacing_curve(history, cat, freq_lit)
            curves[cat] = curve
            levels[cat] = forecast.historical_annual_level(history, cat)
            months = monthly_totals.get(cat, [])
            is_lumpy = forecast.is_lumpy_category(months)
            if is_lumpy:
                lumpy_rates[cat] = forecast.robust_monthly_rate(months)
                lumpy_deviations[cat] = forecast.robust_monthly_deviation(months)
            else:
                this_year_months = (
                    months[-n_completed_this_year:] if n_completed_this_year > 0 else []
                )
                spike_excess[cat] = forecast.spike_excess_this_year(
                    this_year_months, forecast.robust_monthly_rate(months)
                )
            cat_actual = actual[actual["category"] == cat]
            line = forecast.build_forecast_line(
                cat_actual,
                curve,
                year_int,
                as_of,
                freq_lit,
                prior_level=levels[cat],
                k=cfg.forecast_shrinkage_k,
                monthly_rate=lumpy_rates.get(cat),
                monthly_deviation=lumpy_deviations.get(cat),
                seed_key=f"{cat}-{year_int}",
                spike_excess=spike_excess.get(cat, 0.0),
            )
            line = line.copy()
            line["category"] = cat
            parts.append(line)

        if parts:
            pdf_lines = pd.concat(parts, ignore_index=True)
        else:
            pdf_lines = pd.DataFrame(
                columns=["category", "period_end", "cumulative_chf", "segment"]
            )

        spend = data.get_category_year_spend(year_int)
        budgets = data.get_budgets(year_int)
        budgets_filtered = [b for b in budgets if b.category in cats]

        table_rows: list[dict[str, Any]] = []
        if budgets_filtered and not spend.empty:
            table_df = forecast.build_budget_table(
                spend,
                budgets_filtered,
                curves,
                levels,
                year_int,
                as_of,
                k=cfg.forecast_shrinkage_k,
                lumpy_rates=lumpy_rates,
                spike_excess=spike_excess,
            )
            table_rows = table_df.to_dict("records")

        table_card = make_budget_table_card(
            "\U0001f4ca Budget vs Forecast by Category",
            BUDGET_TABLE_COLUMNS,
            table_rows,
            width=12,
        )

        fig = get_fig_BudgetForecast(
            pdf_lines, cfg.npixel_Budget, _fig_helper.vk_Margin
        )
        return fig, table_card
