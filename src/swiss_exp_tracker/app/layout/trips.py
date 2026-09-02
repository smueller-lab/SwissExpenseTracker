from __future__ import annotations

import math

from typing import Any

import pandas as pd

from dash import dcc
from dash import html

from swiss_exp_tracker.app.config import VIS
from swiss_exp_tracker.app.config import config
from swiss_exp_tracker.app.dash_components import GRAPH_CONFIG
from swiss_exp_tracker.app.dash_components import get_balance_class
from swiss_exp_tracker.app.dash_components import make_card_title
from swiss_exp_tracker.app.dash_components import make_page_title

cfg = config()
vis = VIS()


def _trip_options(pdf_Trips: pd.DataFrame) -> list[dict[str, Any]]:
    """Return sorted dropdown options [{label: 'name (year)', value: id}] from pdf_Trips."""
    if pdf_Trips.empty:
        return []
    return [
        {
            "label": f"{row['name']} ({int(row['year'])})",
            "value": int(row["id"]),
        }
        for _, row in pdf_Trips.sort_values("name").iterrows()
    ]


_POOL_PAGE_SIZE = 50


def _filter_pool_search(
    pdf_unassigned: pd.DataFrame, search: str | None
) -> pd.DataFrame:
    """Return rows whose Merchant or category_main contains search (case-insensitive)."""
    if not search or not search.strip():
        return pdf_unassigned
    term = search.strip().lower()
    mask = pdf_unassigned["Merchant"].astype(str).str.lower().str.contains(
        term, na=False, regex=False
    ) | pdf_unassigned["category_main"].astype(str).str.lower().str.contains(
        term, na=False, regex=False
    )
    return pdf_unassigned[mask]


def _build_pool_table(pdf_capped: pd.DataFrame) -> Any:
    """Return an HTML table of draggable, checkboxable, click-to-assign transaction rows."""
    if pdf_capped.empty:
        return html.P("No unassigned transactions.", className="kpi-subtext")

    header = html.Thead(
        html.Tr(
            [
                html.Th("Date"),
                html.Th("Merchant"),
                html.Th("Amount", className="num"),
            ]
        )
    )

    def _row(row: pd.Series) -> Any:
        """Return one <tr> for a single unassigned transaction."""
        tx_id = int(row["id"])
        date_val: pd.Timestamp | object = row["date"]
        date_str = (
            date_val.strftime("%d %b %Y")
            if isinstance(date_val, pd.Timestamp)
            else str(date_val)[:10]
        )
        return html.Tr(
            [
                html.Td(date_str),
                html.Td(str(row["Merchant"])),
                html.Td(f"CHF {float(row['amount_CHF']):,.2f}", className="num"),
            ],
            className="trip-pool-row",
            **{"data-tx-id": str(tx_id)},  # type: ignore[arg-type]
        )

    body = html.Tbody([_row(row) for _, row in pdf_capped.iterrows()])
    return html.Table([header, body], className="simple-table trip-pool-table")


def _pool_table_and_count(
    pdf_unassigned: pd.DataFrame, search: str | None
) -> tuple[Any, str]:
    """Return (table, count-text) for the pool, filtered by search and capped to 50 rows."""
    filtered = _filter_pool_search(pdf_unassigned, search)
    capped = filtered.head(_POOL_PAGE_SIZE)
    table = _build_pool_table(capped)
    if filtered.empty:
        count_text = ""
    elif len(filtered) > len(capped):
        count_text = f"Showing {len(capped)} of {len(filtered)} — refine your search to see more."
    else:
        plural = "s" if len(filtered) != 1 else ""
        count_text = f"{len(filtered)} unassigned transaction{plural}"
    return table, count_text


def _build_trip_body(selected_id: int | None, data: Any) -> Any:
    """Return the detail card for the currently-selected trip, or an empty div."""
    if selected_id is None or data.pdf_Trips.empty:
        return html.Div()

    matching = data.pdf_Trips[data.pdf_Trips["id"] == selected_id]
    if matching.empty:
        return html.Div()

    trip = matching.iloc[0]
    trip_id = int(trip["id"])
    trip_txs = data.pdf_TripTransactionsDetail[
        data.pdf_TripTransactionsDetail["trip_id"] == trip_id
    ]

    def _date_str(val: pd.Timestamp | object) -> str:
        """Return a short display string for a date value."""
        if isinstance(val, pd.Timestamp):
            return str(val.strftime("%d %b %Y"))
        return str(val)[:10]

    tx_rows = [
        html.Div(
            [
                html.Span(_date_str(r["date"]), className="trip-pool-date"),
                html.Span(str(r["Merchant"]), className="trip-pool-merchant"),
                html.Span(
                    f"CHF {float(r['amount_CHF']):,.2f}",
                    className="trip-pool-amount",
                ),
                html.Button(
                    "✕",
                    id={"type": "trip-remove-tx", "index": int(r["transaction_id"])},
                    className="btn-toggle",
                    n_clicks=0,
                ),
            ],
            className="trip-assigned-row",
        )
        for _, r in trip_txs.iterrows()
    ]

    return html.Div(
        [
            html.Div(
                [
                    html.Div(
                        [
                            html.Label("Trip name:", className="budget-field-label"),
                            dcc.Input(
                                id={"type": "trips-rename-input", "index": trip_id},
                                value=str(trip["name"]),
                                type="text",
                                className="budget-number-input",
                            ),
                            html.Button(
                                "Save",
                                id={"type": "trips-rename-btn", "index": trip_id},
                                className="btn-toggle",
                                n_clicks=0,
                            ),
                        ],
                        className="budget-add-row",
                    ),
                    html.Div(
                        [
                            html.Label("Year:", className="budget-field-label"),
                            dcc.Input(
                                id={"type": "trips-year-edit-input", "index": trip_id},
                                value=int(trip["year"]),
                                type="number",
                                min=1990,
                                max=2031,
                                className="budget-number-input",
                            ),
                            html.Button(
                                "Save",
                                id={"type": "trips-year-edit-btn", "index": trip_id},
                                className="btn-toggle",
                                n_clicks=0,
                            ),
                        ],
                        className="budget-add-row",
                    ),
                ],
                className="trip-edit-row",
            ),
            html.P(
                f"Total: CHF {float(trip['total_chf']):,.2f}"
                f" · {int(trip['n_transactions'])} transactions",
                className="kpi-subtext",
            ),
            html.Button(
                f"✕ Delete {trip['name']}",
                id={"type": "trips-delete-btn", "index": trip_id},
                className="btn-toggle",
                n_clicks=0,
            ),
            html.Div(
                (
                    tx_rows
                    if tx_rows
                    else [
                        html.P(
                            "No transactions yet — drag one here, or click one in the list.",
                            className="kpi-subtext",
                        )
                    ]
                ),
                className="trip-bucket-dropzone",
                **{"data-trip-id": str(trip_id)},  # type: ignore[arg-type]
            ),
        ],
        className="trip-bucket-card",
    )


def _bucket_builder_content(data: Any) -> Any:
    """Return the Bucket Builder tab's grid content: pool on the left, trip builder on the right."""
    options = _trip_options(data.pdf_Trips)
    pool_table, pool_count = _pool_table_and_count(
        data.get_unassigned_transactions(), None
    )

    return html.Div(
        [
            # Left card — unassigned transaction pool
            html.Div(
                [
                    make_card_title("Unassigned Transactions"),
                    dcc.Input(
                        id="trips-pool-search",
                        type="text",
                        placeholder="Search merchant or category…",
                        debounce=True,
                        className="trip-pool-search",
                    ),
                    html.P(pool_count, id="trips-pool-count", className="kpi-subtext"),
                    html.Div(
                        pool_table,
                        id="trips-pool-list",
                        className="trips-pool-list",
                    ),
                ],
                className="card col-5",
            ),
            # Right card — single trip builder
            html.Div(
                [
                    # Create-trip form
                    html.Div(
                        [
                            make_card_title("New Trip"),
                            html.Div(
                                [
                                    dcc.Input(
                                        id="trips-create-name",
                                        type="text",
                                        placeholder="Trip name…",
                                        className="budget-number-input",
                                    ),
                                    dcc.Input(
                                        id="trips-create-year",
                                        type="number",
                                        placeholder="Year",
                                        min=1990,
                                        max=2031,
                                        className="budget-number-input",
                                    ),
                                    html.Button(
                                        "+ Add",
                                        id="trips-create-btn",
                                        className="btn-toggle",
                                        disabled=True,
                                        n_clicks=0,
                                    ),
                                ],
                                className="budget-add-row",
                            ),
                            html.Div(
                                id="trips-status-msg", className="budget-save-status"
                            ),
                        ],
                        className="trip-create-form",
                    ),
                    make_card_title("Select Existing Trip"),
                    dcc.Dropdown(
                        id="trips-builder-select",
                        className="dropdown-year dropdown-wide",
                        options=options,  # type: ignore[arg-type]
                        placeholder="Select a trip…",
                        clearable=False,
                    ),
                    html.Div(
                        _build_trip_body(None, data),
                        id="trips-builder-body",
                    ),
                ],
                className="card col-7",
            ),
        ],
        className="grid",
    )


def _kpi_children(
    title: str,
    number: float | int | str | None,
    unit: str = "CHF",
    fmt: str = ",.2f",
    value_class: str | None = None,
) -> list[Any]:
    """Return [card-title, value-paragraph] for a KPI placeholder div's children."""
    if isinstance(number, str):
        cls = value_class if value_class is not None else get_balance_class(None)
        return [make_card_title(title), html.P(number, className=cls)]
    is_missing = number is None or (isinstance(number, float) and math.isnan(number))
    cls = (
        value_class
        if value_class is not None
        else get_balance_class(
            None if is_missing else (float(number) if number is not None else None)
        )
    )
    if is_missing:
        text = "—"
    else:
        suffix = f" {unit}" if unit else ""
        text = f"{number:{fmt}}{suffix}"
    return [make_card_title(title), html.P(text, className=cls)]


def _overview_kpi_values(
    pdf_Trips: pd.DataFrame,
) -> tuple[list[Any], list[Any], list[Any], list[Any]]:
    """Return four KPI children tuples: n_trips, total_chf, avg_chf, priciest_name."""
    if pdf_Trips.empty:
        return (
            _kpi_children("Total Trips", 0, unit="", fmt=",d"),
            _kpi_children("Total Spent", 0.0),
            _kpi_children("Avg / Trip", None),
            _kpi_children("Priciest Trip", "—"),
        )
    n_trips = len(pdf_Trips)
    total_chf = float(pdf_Trips["total_chf"].sum())
    avg_chf = total_chf / n_trips if n_trips > 0 else None
    priciest = str(pdf_Trips.loc[pdf_Trips["total_chf"].idxmax(), "name"])
    return (
        _kpi_children("Total Trips", n_trips, unit="", fmt=",d"),
        _kpi_children("Total Spent", total_chf),
        _kpi_children("Avg / Trip", avg_chf),
        _kpi_children("Priciest Trip", priciest),
    )


def _trips_overview_content(data: Any) -> Any:
    """Return the Trips Overview tab's grid content: KPIs, bar chart, and drill-down donut."""
    options = _trip_options(data.pdf_Trips)
    kpi_n, kpi_total, kpi_avg, kpi_priciest = _overview_kpi_values(data.pdf_Trips)

    return html.Div(
        [
            # KPI row -- 4x col-3 = 12
            html.Div(kpi_n, id="trips-kpi-n-trips", className="card card-kpi col-3"),
            html.Div(
                kpi_total, id="trips-kpi-total-chf", className="card card-kpi col-3"
            ),
            html.Div(kpi_avg, id="trips-kpi-avg-chf", className="card card-kpi col-3"),
            html.Div(
                kpi_priciest, id="trips-kpi-priciest", className="card card-kpi col-3"
            ),
            # Bar chart — col-12
            html.Div(
                [
                    make_card_title("Cost per Trip by Year [CHF]"),
                    dcc.Loading(
                        dcc.Graph(
                            id="trips-bar-fig",
                            figure={},
                            config=GRAPH_CONFIG,
                        )
                    ),
                ],
                className="card card-graph col-12",
            ),
            # Drill-down selector — col-4
            html.Div(
                [
                    make_card_title("Trip Details"),
                    dcc.Dropdown(
                        id="trips-overview-trip-select",
                        className="dropdown-year",
                        options=options,  # type: ignore[arg-type]
                        placeholder="Select a trip…",
                    ),
                    html.Div(id="trips-overview-mini-kpi"),
                ],
                className="card card-kpi col-4",
            ),
            # Donut — col-8
            html.Div(
                [
                    make_card_title("Spend by Category"),
                    dcc.Loading(
                        dcc.Graph(
                            id="trips-donut-fig",
                            figure={},
                            config=GRAPH_CONFIG,
                        )
                    ),
                ],
                className="card card-graph col-8",
            ),
        ],
        className="grid",
    )


def layout(data: Any) -> Any:
    """Return the /trips page layout: two-tab view with Bucket Builder and Trips Overview."""
    return html.Div(
        [
            make_page_title("\U0001f9f3 Trip Buckets"),
            # Stores live OUTSIDE dcc.Tabs so they persist across tab switches
            dcc.Store(id="trips-drop-store"),
            dcc.Store(id="trips-version", data=0),
            dcc.Store(id="trips-builder-selected-id"),
            dcc.Tabs(
                [
                    dcc.Tab(
                        label="Bucket Builder",
                        className="trips-tab",
                        selected_className="trips-tab trips-tab--selected",
                        style={},
                        selected_style={},
                        children=_bucket_builder_content(data),
                    ),
                    dcc.Tab(
                        label="Trips Overview",
                        className="trips-tab",
                        selected_className="trips-tab trips-tab--selected",
                        style={},
                        selected_style={},
                        children=_trips_overview_content(data),
                    ),
                ],
                className="trips-tabs",
            ),
        ]
    )
