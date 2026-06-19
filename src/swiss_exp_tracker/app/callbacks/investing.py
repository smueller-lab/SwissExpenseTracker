from __future__ import annotations

import datetime

from typing import Any

from dash import Input
from dash import Output
from dash import html

from swiss_exp_tracker.app.dash_components import get_balance_class
from swiss_exp_tracker.app.dash_components import make_card_title
from swiss_exp_tracker.app.vis.figure_investing import fig_position_pct
from swiss_exp_tracker.app.vis.figure_investing import fig_position_value


def register_callbacks(app: Any, data: Any, pos: Any) -> None:
    @app.callback(  # type: ignore[untyped-decorator]
        Output("investing-performers-content", "children"),
        Input("investing-year-scope", "value"),
    )
    def update_performers(scope: str) -> Any:  # pyright: ignore[reportUnusedFunction]
        pdf = pos.pdf.copy()

        if scope == "year":
            current_year = datetime.date.today().year
            pdf = pdf[pdf["date"].dt.year == current_year]

        if pdf.empty:
            return html.P(
                "No Swissquote data found. Import your positions data first.",
                className="kpi-subtext",
            )

        latest = pdf["date"].max()
        pdf_snap = pdf[pdf["date"] == latest]

        best_row = pdf_snap.loc[pdf_snap["pnl_pct"].idxmax()]
        worst_row = pdf_snap.loc[pdf_snap["pnl_pct"].idxmin()]

        def _block(row: Any, title: str) -> Any:
            pnl_pct_val = float(row["pnl_pct"]) * 100
            pnl_chf_val = float(row["pnl_chf"])
            name = row["name"] if row["name"] else None
            return html.Div(
                [
                    make_card_title(title),
                    html.P(str(row["symbol"]), className="kpi-category"),
                    html.P(name, className="kpi-subtext") if name else None,
                    html.P(
                        f"{pnl_pct_val:+.2f} %",
                        className=get_balance_class(pnl_pct_val),
                    ),
                    html.P(
                        f"{pnl_chf_val:+,.2f} CHF",
                        className=f"kpi-subtext {get_balance_class(pnl_chf_val)}",
                    ),
                ],
                className="kpi-block",
            )

        return [
            _block(best_row, "Best"),
            html.Hr(className="kpi-divider"),
            _block(worst_row, "Worst"),
            html.P(latest.strftime("%Y-%m-%d"), className="kpi-subtext"),
        ]

    @app.callback(  # type: ignore[untyped-decorator]
        Output("investing-pos-value", "figure"),
        Output("investing-pos-pct", "figure"),
        Input("investing-symbol-dropdown", "value"),
        Input("investing-year-scope", "value"),
    )
    def update_position_charts(  # pyright: ignore[reportUnusedFunction]
        symbols: list[str] | None,
        scope: str,
    ) -> tuple[Any, Any]:
        if not symbols:
            symbols = pos.all_symbols

        pdf = pos.pdf.copy()
        if scope == "year":
            current_year = datetime.date.today().year
            pdf = pdf[pdf["date"].dt.year == current_year]

        return fig_position_value(pdf, symbols), fig_position_pct(pdf, symbols)
