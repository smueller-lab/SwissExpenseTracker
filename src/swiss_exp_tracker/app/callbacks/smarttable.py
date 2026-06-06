from __future__ import annotations

from typing import Any

from dash import Input
from dash import Output
from dash import html

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


def register_callbacks(app: Any, data: Any) -> None:
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

        s_col = ["Date", "Merchant", "category_main", "category_second", "amount_CHF"]
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
