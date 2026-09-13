from __future__ import annotations

from typing import Any

from dash import Input
from dash import Output
from dash import State
from dash import html
from dash import no_update
from dash.exceptions import PreventUpdate

from swiss_exp_tracker.app.layout.smarttable import smart_table_columns

_EDITABLE_COLUMNS = ("Merchant", "category_main", "category_second")

_EXCLUDED_SECOND = frozenset(
    [
        "Money Transfer",
        "Donation",
        "Brokerage",
        "Taxes",
        "Deposit",
        "Rent",
        "Payment Fees",
        "Health Insurance",
        "Transfer Fees",
    ]
)

_EXCLUDED_MAIN = frozenset(["Insurance", "Salary"])


def _find_single_edit(
    table_data: list[dict[str, Any]], table_data_previous: list[dict[str, Any]]
) -> tuple[dict[str, Any], dict[str, Any], str] | None:
    """Return (row, prev_row, changed_column) if exactly one editable cell differs
    between table_data and table_data_previous (rows matched by hidden `id`);
    None otherwise. Rejects ambiguous multi-cell diffs (e.g. a paste, or a
    same-value echo from this callback's own previous output) by requiring
    exactly one (row, column) pair to differ.
    """
    prev_by_id = {row["id"]: row for row in table_data_previous}
    edits: list[tuple[dict[str, Any], dict[str, Any], str]] = []
    for row in table_data:
        prev_row = prev_by_id.get(row["id"])
        if prev_row is None:
            continue
        for col in _EDITABLE_COLUMNS:
            if prev_row.get(col) != row.get(col):
                edits.append((row, prev_row, col))
    if len(edits) != 1:
        return None
    return edits[0]


def register_callbacks(app: Any, data: Any) -> None:
    @app.callback(  # type: ignore[untyped-decorator]
        Output("smart-table", "editable"),
        Output("smart-table", "columns"),
        Input("smart-edit-toggle", "value"),
    )
    def toggle_edit_mode(
        edit_toggle: list[str] | None,
    ) -> tuple[bool, list[dict[str, Any]]]:  # pyright: ignore[reportUnusedFunction]
        """Gate table + per-column editability on the "Edit table" checkbox; off by default.
        Column-level `editable` is what Dash actually enforces per cell, so the
        table-level flag alone isn't enough — both must be rebuilt together.
        """
        is_editable = "edit" in (edit_toggle or [])
        return is_editable, smart_table_columns(editable=is_editable)

    @app.callback(  # type: ignore[untyped-decorator]
        Output("smart-cat-second", "options"),
        Output("smart-merchant", "options"),
        Input("smart-cat-main", "value"),
        Input("smart-cat-second", "value"),
    )
    def update_cascading_options(  # pyright: ignore[reportUnusedFunction]
        cat_main: list[str] | None,
        cat_second: list[str] | None,
    ) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
        pdf = data.pdf_Master.copy()
        pdf = pdf[pdf["transaction_type"] == "EXPENSE"]

        if cat_main:
            pdf = pdf[pdf["category_main"].isin(cat_main)]

        second_opts = [
            {"label": c, "value": c}
            for c in sorted(pdf["category_second"].dropna().unique().tolist())
        ]

        if cat_second:
            pdf = pdf[pdf["category_second"].isin(cat_second)]

        merchant_opts = [
            {"label": m, "value": m}
            for m in sorted(pdf["Merchant"].dropna().unique().tolist())
        ]

        return second_opts, merchant_opts

    @app.callback(  # type: ignore[untyped-decorator]
        Output("smart-table", "data"),
        Output("smart-summary", "children"),
        Input("smart-year-start", "value"),
        Input("smart-month-start", "value"),
        Input("smart-year-end", "value"),
        Input("smart-month-end", "value"),
        Input("smart-cat-main", "value"),
        Input("smart-cat-second", "value"),
        Input("smart-merchant", "value"),
        Input("smart-amount-range", "value"),
        Input("smart-exclude-check", "value"),
    )
    def update_table(  # pyright: ignore[reportUnusedFunction]
        year_start: int | None,
        month_start: int | None,
        year_end: int | None,
        month_end: int | None,
        cat_main: list[str] | None,
        cat_second: list[str] | None,
        merchants: list[str] | None,
        amount_range: list[float] | None,
        exclude_val: list[str] | None,
    ) -> tuple[list[dict[str, Any]], list[Any]]:
        pdf = data.pdf_Master.copy()
        pdf = pdf[pdf["transaction_type"] == "EXPENSE"]

        if year_start is not None:
            if month_start is not None:
                pdf = pdf[
                    (pdf["date"].dt.year > int(year_start))
                    | (
                        (pdf["date"].dt.year == int(year_start))
                        & (pdf["date"].dt.month >= int(month_start))
                    )
                ]
            else:
                pdf = pdf[pdf["date"].dt.year >= int(year_start)]

        if year_end is not None:
            if month_end is not None:
                pdf = pdf[
                    (pdf["date"].dt.year < int(year_end))
                    | (
                        (pdf["date"].dt.year == int(year_end))
                        & (pdf["date"].dt.month <= int(month_end))
                    )
                ]
            else:
                pdf = pdf[pdf["date"].dt.year <= int(year_end)]

        if "exclude" in (exclude_val or []):
            pdf = pdf[~pdf["category_second"].isin(_EXCLUDED_SECOND)]
            pdf = pdf[~pdf["category_main"].isin(_EXCLUDED_MAIN)]

        if cat_main:
            pdf = pdf[pdf["category_main"].isin(cat_main)]
        if cat_second:
            pdf = pdf[pdf["category_second"].isin(cat_second)]

        if merchants:
            pdf = pdf[pdf["Merchant"].isin(merchants)]

        if amount_range:
            pdf = pdf[
                (pdf["amount_CHF"] >= amount_range[0])
                & (pdf["amount_CHF"] <= amount_range[1])
            ]

        pdf = pdf.sort_values("date", ascending=False)
        pdf["Date"] = pdf["date"].dt.strftime("%d-%m-%Y")

        # id/reference aren't in the DataTable's `columns` config, so they render
        # nowhere, but stay in `data` for the edit callback to target the right row.
        s_col = [
            "id",
            "reference",
            "Date",
            "Merchant",
            "category_main",
            "category_second",
            "amount_CHF",
        ]
        pdf_out = pdf[s_col].copy()

        n = len(pdf_out)
        total = pdf_out["amount_CHF"].sum()
        avg = total / n if n > 0 else 0.0

        summary: list[Any] = [
            html.Span(f"{n:,}", className="summary-stat-value"),
            html.Span(" transactions", className="summary-stat-label"),
            html.Span(" · ", className="summary-sep"),
            html.Span(f"CHF {total:,.2f}", className="summary-stat-value"),
            html.Span(" total", className="summary-stat-label"),
            html.Span(" · ", className="summary-sep"),
            html.Span(f"CHF {avg:,.2f}", className="summary-stat-value"),
            html.Span(" avg / transaction", className="summary-stat-label"),
        ]

        return pdf_out.to_dict("records"), summary

    @app.callback(  # type: ignore[untyped-decorator]
        Output("smart-table", "data", allow_duplicate=True),
        Output("smart-table-alert", "children"),
        Output("smart-table-alert", "className"),
        Input("smart-table", "data"),
        State("smart-table", "data_previous"),
        prevent_initial_call=True,
    )
    def handle_cell_edit(  # pyright: ignore[reportUnusedFunction]
        table_data: list[dict[str, Any]] | None,
        table_data_previous: list[dict[str, Any]] | None,
    ) -> tuple[list[dict[str, Any]] | Any, str, str]:
        if not table_data or not table_data_previous:
            raise PreventUpdate

        edit = _find_single_edit(table_data, table_data_previous)
        if edit is None:
            raise PreventUpdate

        row, prev_row, col = edit
        new_value = str(row.get(col) or "").strip()

        if not new_value:
            label = {
                "Merchant": "Merchant",
                "category_main": "Category",
                "category_second": "Subcategory",
            }[col]
            reverted = [dict(r) for r in table_data]
            for r in reverted:
                if r["id"] == row["id"]:
                    r[col] = prev_row[col]
            return (
                reverted,
                f"{label} must not be empty",
                "smart-table-alert smart-table-alert-error",
            )

        # The sibling category field is untouched by this edit; a None value
        # (no subcategory in the DB) must not propagate into recategorize_merchant.
        category_main = str(prev_row["category_main"] or "")
        category_second = str(prev_row["category_second"] or "")
        try:
            if col == "Merchant":
                data.rename_merchant(prev_row["reference"], new_value)
            else:
                if col == "category_main":
                    category_main = new_value
                else:
                    category_second = new_value
                data.recategorize_merchant(
                    prev_row["Merchant"], category_main, category_second
                )
        except ValueError as exc:
            reverted = [dict(r) for r in table_data]
            for r in reverted:
                if r["id"] == row["id"]:
                    r[col] = prev_row[col]
            return reverted, str(exc), "smart-table-alert smart-table-alert-error"
        except RuntimeError as exc:
            alert_children, alert_class = (
                str(exc),
                "smart-table-alert smart-table-alert-warning",
            )
        else:
            alert_children, alert_class = "", "smart-table-alert"

        if col == "Merchant":
            # Row-scoped: the cell the user typed is already correct in table_data.
            return no_update, alert_children, alert_class

        category_second_display = category_second or None
        merchant_key = str(prev_row["Merchant"]).lower()
        updated = [dict(r) for r in table_data]
        changed_any = False
        for r in updated:
            if str(r["Merchant"]).lower() == merchant_key and (
                r["category_main"] != category_main
                or r["category_second"] != category_second_display
            ):
                r["category_main"] = category_main
                r["category_second"] = category_second_display
                changed_any = True

        return (updated if changed_any else no_update), alert_children, alert_class
