from __future__ import annotations

import sqlite3

from typing import Any

import plotly.graph_objects as go

from dash import ALL
from dash import Input
from dash import Output
from dash import State
from dash import ctx
from dash import html
from dash import no_update
from pydantic import ValidationError

from swiss_exp_tracker.app.config import VIS
from swiss_exp_tracker.app.dash_components import get_balance_class
from swiss_exp_tracker.app.layout.trips import _POOL_PAGE_SIZE
from swiss_exp_tracker.app.layout.trips import _build_trip_body
from swiss_exp_tracker.app.layout.trips import _overview_kpi_values
from swiss_exp_tracker.app.layout.trips import _pool_table_and_count
from swiss_exp_tracker.app.layout.trips import _pool_year_options
from swiss_exp_tracker.app.layout.trips import _show_more_class
from swiss_exp_tracker.app.layout.trips import _show_more_label
from swiss_exp_tracker.app.layout.trips import _trip_options
from swiss_exp_tracker.app.vis.figure import Fig

_F = Fig()
vis = VIS()


def register_callbacks(app: Any, data: Any) -> None:
    """Register all callbacks for the /trips page."""

    @app.callback(  # type: ignore[untyped-decorator]
        Output("trips-create-btn", "disabled"),
        Input("trips-create-name", "value"),
        Input("trips-create-year", "value"),
    )
    def toggle_create_btn(  # pyright: ignore[reportUnusedFunction]
        name: str | None, year: int | None
    ) -> bool:
        """Disable the Add button until both name and year are non-empty."""
        return not (bool(name and str(name).strip()) and year is not None)

    @app.callback(  # type: ignore[untyped-decorator]
        Output("trips-version", "data"),
        Output("trips-builder-select", "value"),
        Output("trips-builder-selected-id", "data"),
        Output("trips-status-msg", "children"),
        Input("trips-drop-store", "data"),
        Input("trips-create-btn", "n_clicks"),
        Input({"type": "trips-rename-btn", "index": ALL}, "n_clicks"),
        Input({"type": "trips-year-edit-btn", "index": ALL}, "n_clicks"),
        Input({"type": "trips-delete-btn", "index": ALL}, "n_clicks"),
        Input({"type": "trip-remove-tx", "index": ALL}, "n_clicks"),
        Input({"type": "trip-split-input", "index": ALL}, "value"),
        State("trips-version", "data"),
        State("trips-builder-select", "value"),
        State("trips-create-name", "value"),
        State("trips-create-year", "value"),
        State({"type": "trips-rename-input", "index": ALL}, "value"),
        State({"type": "trips-rename-btn", "index": ALL}, "id"),
        State({"type": "trips-year-edit-input", "index": ALL}, "value"),
        State({"type": "trips-year-edit-btn", "index": ALL}, "id"),
        State({"type": "trips-delete-btn", "index": ALL}, "id"),
        State({"type": "trip-split-input", "index": ALL}, "id"),
        prevent_initial_call=True,
    )
    def mutate_trips(  # pyright: ignore[reportUnusedFunction]
        drop_data: dict[str, Any] | None,
        _create_clicks: int | None,
        rename_clicks: list[int | None],
        year_edit_clicks: list[int | None],
        delete_clicks: list[int | None],
        remove_clicks: list[int | None],
        split_values: list[int | None],
        version: int,
        builder_selected: int | None,
        create_name: str | None,
        create_year: int | None,
        rename_values: list[str | None],
        rename_ids: list[dict[str, Any]],
        year_edit_values: list[int | None],
        year_edit_ids: list[dict[str, Any]],
        delete_ids: list[dict[str, Any]],
        split_ids: list[dict[str, Any]],
    ) -> tuple[Any, Any, Any, Any]:
        """Branch on ctx.triggered_id to mutate trips; bump version to trigger re-render."""
        triggered = ctx.triggered_id
        new_version = (version or 0) + 1
        _no_select = no_update
        _no_store = no_update

        try:
            # --- Drop event (drag-and-drop) ---
            if triggered == "trips-drop-store":
                if drop_data is None:
                    return no_update, no_update, no_update, no_update
                tx_id = int(drop_data["tx_id"])
                trip_id = int(drop_data["trip_id"])
                data.assign_transactions_to_trip(trip_id, [tx_id])
                return new_version, no_update, no_update, ""

            # --- Create trip ---
            if triggered == "trips-create-btn":
                if not create_name or create_year is None:
                    return no_update, no_update, no_update, no_update
                data.create_trip(str(create_name).strip(), int(create_year))
                # Find the newly created trip's id by name
                new_id: int | None = None
                if not data.pdf_Trips.empty:
                    match = data.pdf_Trips[
                        data.pdf_Trips["name"] == str(create_name).strip()
                    ]
                    if not match.empty:
                        new_id = int(match.iloc[0]["id"])
                return new_version, new_id, new_id, f"✓ Trip '{create_name}' created"

            # --- Rename trip ---
            if (
                isinstance(triggered, dict)
                and triggered.get("type") == "trips-rename-btn"
            ):
                trip_id = int(triggered["index"])
                idx = next(
                    (i for i, d in enumerate(rename_ids) if d.get("index") == trip_id),
                    None,
                )
                if idx is None:
                    return no_update, no_update, no_update, no_update
                new_name = rename_values[idx]
                if not new_name:
                    return no_update, no_update, no_update, "✗ Name must not be empty."
                data.rename_trip(trip_id, str(new_name))
                return new_version, _no_select, _no_store, f"✓ Renamed to '{new_name}'"

            # --- Update year ---
            if (
                isinstance(triggered, dict)
                and triggered.get("type") == "trips-year-edit-btn"
            ):
                trip_id = int(triggered["index"])
                idx = next(
                    (
                        i
                        for i, d in enumerate(year_edit_ids)
                        if d.get("index") == trip_id
                    ),
                    None,
                )
                if idx is None:
                    return no_update, no_update, no_update, no_update
                new_year = year_edit_values[idx]
                if new_year is None:
                    return no_update, no_update, no_update, "✗ Year must not be empty."
                data.update_trip_year(trip_id, int(new_year))
                return (
                    new_version,
                    _no_select,
                    _no_store,
                    f"✓ Year updated to {new_year}",
                )

            # --- Delete trip ---
            if (
                isinstance(triggered, dict)
                and triggered.get("type") == "trips-delete-btn"
            ):
                trip_id = int(triggered["index"])
                data.delete_trip(trip_id)
                return new_version, None, None, "✓ Trip deleted"

            # --- Per-row remove ---
            if (
                isinstance(triggered, dict)
                and triggered.get("type") == "trip-remove-tx"
            ):
                tx_id = int(triggered["index"])
                data.unassign_transactions([tx_id])
                return new_version, _no_select, _no_store, ""

            # --- Per-row split update ---
            if (
                isinstance(triggered, dict)
                and triggered.get("type") == "trip-split-input"
            ):
                tx_id = int(triggered["index"])
                idx = next(
                    (i for i, d in enumerate(split_ids) if d.get("index") == tx_id),
                    None,
                )
                if idx is None or split_values[idx] is None:
                    return no_update, no_update, no_update, no_update
                new_split = int(split_values[idx])  # type: ignore[arg-type]
                data.update_transaction_split(tx_id, new_split)
                return new_version, _no_select, _no_store, f"✓ Split set to {new_split}"

        except (sqlite3.IntegrityError, sqlite3.DatabaseError) as exc:
            msg = str(exc)
            if "UNIQUE" in msg.upper() or "unique" in msg:
                return (
                    no_update,
                    no_update,
                    no_update,
                    "✗ A trip with that name already exists for that year.",
                )
            return no_update, no_update, no_update, f"✗ Database error: {msg}"
        except ValidationError as exc:
            first_msg = exc.errors()[0]["msg"] if exc.errors() else str(exc)
            return no_update, no_update, no_update, f"✗ {first_msg}"

        return no_update, no_update, no_update, no_update

    @app.callback(  # type: ignore[untyped-decorator]
        Output("trips-pool-limit", "data"),
        Input("trips-pool-show-more", "n_clicks"),
        Input("trips-pool-search", "value"),
        Input("trips-pool-year", "value"),
        Input("trips-pool-month", "value"),
        State("trips-pool-limit", "data"),
        prevent_initial_call=True,
    )
    def update_pool_limit(  # pyright: ignore[reportUnusedFunction]
        _n_clicks: int | None,
        _search: str | None,
        _pool_year: int | None,
        _pool_month: int | None,
        limit: int | None,
    ) -> int:
        """Grow the pool row limit by one page on 'Show more'; reset to the default on any filter change."""
        if ctx.triggered_id == "trips-pool-show-more":
            return (limit or _POOL_PAGE_SIZE) + _POOL_PAGE_SIZE
        return _POOL_PAGE_SIZE

    @app.callback(  # type: ignore[untyped-decorator]
        Output("trips-pool-list", "children"),
        Output("trips-pool-count", "children"),
        Output("trips-pool-show-more", "children"),
        Output("trips-pool-show-more", "className"),
        Output("trips-builder-body", "children"),
        Output("trips-builder-select", "options"),
        Output("trips-pool-year", "options"),
        Input("trips-version", "data"),
        Input("trips-builder-select", "value"),
        Input("trips-pool-search", "value"),
        Input("trips-pool-year", "value"),
        Input("trips-pool-month", "value"),
        Input("trips-pool-limit", "data"),
    )
    def render_bucket_builder(  # pyright: ignore[reportUnusedFunction]
        _version: int | None,
        selected_id: int | None,
        search: str | None,
        pool_year: int | None,
        pool_month: int | None,
        pool_limit: int | None,
    ) -> tuple[Any, str, str, str, Any, list[dict[str, Any]], list[dict[str, Any]]]:
        """Rebuild the pool table (filtered by search/year/month, capped to pool_limit) and selected trip card."""
        options = _trip_options(data.pdf_Trips)
        unassigned = data.get_unassigned_transactions()
        pool_table, pool_count, pool_remaining = _pool_table_and_count(
            unassigned, search, pool_year, pool_month, pool_limit or _POOL_PAGE_SIZE
        )
        trip_body = _build_trip_body(selected_id, data)
        year_options = _pool_year_options(unassigned)
        return (
            pool_table,
            pool_count,
            _show_more_label(pool_remaining),
            _show_more_class(pool_remaining),
            trip_body,
            options,
            year_options,
        )

    @app.callback(  # type: ignore[untyped-decorator]
        Output("trips-kpi-n-trips", "children"),
        Output("trips-kpi-total-chf", "children"),
        Output("trips-kpi-avg-chf", "children"),
        Output("trips-kpi-priciest", "children"),
        Output("trips-bar-fig", "figure"),
        Output("trips-overview-mini-kpi", "children"),
        Output("trips-donut-fig", "figure"),
        Output("trips-overview-trip-select", "options"),
        Input("trips-version", "data"),
        Input("trips-overview-trip-select", "value"),
    )
    def render_trips_overview(  # pyright: ignore[reportUnusedFunction]
        _version: int | None,
        selected_trip_id: int | None,
    ) -> tuple[Any, Any, Any, Any, Any, Any, Any, list[dict[str, Any]]]:
        """Rebuild the KPI row, bar chart, and drill-down donut for the overview tab."""
        options = _trip_options(data.pdf_Trips)
        kpi_n, kpi_total, kpi_avg, kpi_priciest = _overview_kpi_values(data.pdf_Trips)
        fig_bar = _F.fig_BarTripCostByYear(data.pdf_TripsByCategoryYear)

        if selected_trip_id is None or data.pdf_Trips.empty:
            return (
                kpi_n,
                kpi_total,
                kpi_avg,
                kpi_priciest,
                fig_bar,
                html.Div(),
                go.Figure(),
                options,
            )

        trip_match = data.pdf_Trips[data.pdf_Trips["id"] == selected_trip_id]
        if trip_match.empty:
            return (
                kpi_n,
                kpi_total,
                kpi_avg,
                kpi_priciest,
                fig_bar,
                html.Div(),
                go.Figure(),
                options,
            )

        trip = trip_match.iloc[0]
        trip_txs = data.pdf_TripTransactionsDetail[
            data.pdf_TripTransactionsDetail["trip_id"] == selected_trip_id
        ]

        fig_donut: go.Figure
        if trip_txs.empty:
            fig_donut = go.Figure()
        else:
            fig_donut = _F.fig_DonutByCategory(
                trip_txs,
                col_category="category_main",
                col_amount="share_CHF",
                col_map=vis.vk_CategoryMain_col,
            )

        mini_kpi = html.Div(
            [
                html.P(
                    f"CHF {float(trip['total_chf']):,.2f}",
                    className=get_balance_class(None),
                ),
                html.P(
                    f"{int(trip['n_transactions'])} transactions",
                    className="kpi-subtext",
                ),
            ]
        )

        return (
            kpi_n,
            kpi_total,
            kpi_avg,
            kpi_priciest,
            fig_bar,
            mini_kpi,
            fig_donut,
            options,
        )
