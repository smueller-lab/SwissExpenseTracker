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


_POOL_PAGE_SIZE = 100

_MONTH_NAMES = [
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
]


def _pool_year_options(pdf_unassigned: pd.DataFrame) -> list[dict[str, Any]]:
    """Return sorted-descending {label, value} year options from unassigned transaction dates."""
    if pdf_unassigned.empty:
        return []
    years = sorted(
        pdf_unassigned["date"].dropna().dt.year.astype(int).unique().tolist(),
        reverse=True,
    )
    return [{"label": str(y), "value": y} for y in years]


def _pool_month_options() -> list[dict[str, Any]]:
    """Return the fixed [{label, value}] month options, value 1-12."""
    return [{"label": m, "value": i} for i, m in enumerate(_MONTH_NAMES, start=1)]


def _filter_pool_search(
    pdf_unassigned: pd.DataFrame,
    search: str | None,
    year: int | None = None,
    month: int | None = None,
) -> pd.DataFrame:
    """Return rows matching search (Merchant/category_main) and, if set, year/month of date."""
    result = pdf_unassigned
    if year is not None:
        result = result[result["date"].dt.year == year]
    if month is not None:
        result = result[result["date"].dt.month == month]
    if search and search.strip():
        term = search.strip().lower()
        mask = result["Merchant"].astype(str).str.lower().str.contains(
            term, na=False, regex=False
        ) | result["category_main"].astype(str).str.lower().str.contains(
            term, na=False, regex=False
        )
        result = result[mask]
    return result


def _build_pool_table(pdf_capped: pd.DataFrame) -> Any:
    """Return an HTML table of draggable, checkboxable, click-to-assign transaction rows."""
    if pdf_capped.empty:
        return html.P("No unassigned transactions.", className="kpi-subtext")

    header = html.Thead(
        html.Tr(
            [
                html.Th("Date", className="date"),
                html.Th("Merchant"),
                html.Th("Amount [CHF]", className="num"),
            ]
        )
    )

    def _row(row: pd.Series) -> Any:
        """Return one <tr> for a single unassigned transaction, amount signed/colored by transaction_type."""
        tx_id = int(row["id"])
        date_val: pd.Timestamp | object = row["date"]
        date_str = (
            date_val.strftime("%d %b %Y")
            if isinstance(date_val, pd.Timestamp)
            else str(date_val)[:10]
        )
        is_income = row["transaction_type"] == "INCOME"
        sign = "+" if is_income else "-"
        amount_class = "num " + (
            "balance-positive" if is_income else "balance-negative"
        )
        return html.Tr(
            [
                html.Td(date_str, className="date"),
                html.Td(str(row["Merchant"])),
                html.Td(
                    f"{sign} {float(row['amount_CHF']):,.2f}", className=amount_class
                ),
            ],
            className="trip-pool-row",
            **{"data-tx-id": str(tx_id)},  # type: ignore[arg-type]
        )

    body = html.Tbody([_row(row) for _, row in pdf_capped.iterrows()])
    return html.Table([header, body], className="simple-table trip-pool-table")


def _pool_table_and_count(
    pdf_unassigned: pd.DataFrame,
    search: str | None,
    year: int | None = None,
    month: int | None = None,
    limit: int = _POOL_PAGE_SIZE,
) -> tuple[Any, str, int]:
    """Return (table, count-text, remaining-row-count) for the pool, capped to limit rows."""
    filtered = _filter_pool_search(pdf_unassigned, search, year, month)
    capped = filtered.head(limit)
    table = _build_pool_table(capped)
    remaining = len(filtered) - len(capped)
    if filtered.empty:
        count_text = ""
    elif remaining > 0:
        count_text = f"Showing {len(capped)} of {len(filtered)} unassigned transactions"
    else:
        plural = "s" if len(filtered) != 1 else ""
        count_text = f"{len(filtered)} unassigned transaction{plural}"
    return table, count_text, remaining


def _show_more_label(remaining: int) -> str:
    """Return the 'Show more' button label for the given remaining row count."""
    return f"Show {min(remaining, _POOL_PAGE_SIZE)} more"


def _show_more_class(remaining: int) -> str:
    """Return the 'Show more' button className, hidden via is-hidden when nothing remains."""
    base = "btn-toggle trip-pool-show-more-btn"
    return base if remaining > 0 else f"{base} is-hidden"


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

    def _tx_row(r: pd.Series) -> Any:
        """Return one assigned-transaction row: amount signed/colored by type, plus an editable people-split input."""
        tx_id = int(r["transaction_id"])
        is_income = r["transaction_type"] == "INCOME"
        sign = "+" if is_income else "-"
        amount_class = "trip-pool-amount " + (
            "balance-positive" if is_income else "balance-negative"
        )
        return html.Div(
            [
                html.Span(_date_str(r["date"]), className="trip-pool-date"),
                html.Span(str(r["Merchant"]), className="trip-pool-merchant"),
                html.Span(
                    f"{sign} {float(r['amount_CHF']):,.2f}",
                    className=amount_class,
                ),
                html.Span("÷", className="trip-split-symbol"),
                dcc.Input(
                    id={"type": "trip-split-input", "index": tx_id},
                    type="number",
                    min=1,
                    max=20,
                    step=1,
                    value=int(r["split"]),
                    debounce=True,
                    className="trip-split-input",
                ),
                html.Button(
                    "✕",
                    id={"type": "trip-remove-tx", "index": tx_id},
                    className="btn-toggle",
                    n_clicks=0,
                ),
            ],
            className="trip-assigned-row",
        )

    expense_rows = [
        _tx_row(r)
        for _, r in trip_txs[trip_txs["transaction_type"] == "EXPENSE"].iterrows()
    ]
    income_rows = [
        _tx_row(r)
        for _, r in trip_txs[trip_txs["transaction_type"] == "INCOME"].iterrows()
    ]
    if expense_rows and income_rows:
        tx_rows = [
            *expense_rows,
            html.Hr(className="trip-income-divider"),
            *income_rows,
        ]
    else:
        tx_rows = [*expense_rows, *income_rows]

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
    unassigned = data.get_unassigned_transactions()
    pool_table, pool_count, pool_remaining = _pool_table_and_count(unassigned, None)
    year_options = _pool_year_options(unassigned)
    month_options = _pool_month_options()

    return html.Div(
        [
            # Left card — unassigned transaction pool
            html.Div(
                [
                    dcc.Store(id="trips-pool-limit", data=_POOL_PAGE_SIZE),
                    make_card_title("Unassigned Transactions"),
                    html.Div(
                        [
                            dcc.Input(
                                id="trips-pool-search",
                                type="text",
                                placeholder="Search merchant or category…",
                                debounce=True,
                                className="trip-pool-search",
                            ),
                            dcc.Dropdown(
                                id="trips-pool-year",
                                options=year_options,  # type: ignore[arg-type]
                                placeholder="Year",
                                clearable=True,
                                className="dropdown-year trip-pool-year-dd",
                            ),
                            dcc.Dropdown(
                                id="trips-pool-month",
                                options=month_options,  # type: ignore[arg-type]
                                placeholder="Month",
                                clearable=True,
                                className="dropdown-year trip-pool-month-dd",
                            ),
                        ],
                        className="trip-pool-filter-row",
                    ),
                    html.P(pool_count, id="trips-pool-count", className="kpi-subtext"),
                    html.Div(
                        pool_table,
                        id="trips-pool-list",
                        className="trips-pool-list",
                    ),
                    html.Button(
                        _show_more_label(pool_remaining),
                        id="trips-pool-show-more",
                        className=_show_more_class(pool_remaining),
                        n_clicks=0,
                    ),
                ],
                className="card col-6",
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
                className="card col-6",
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
