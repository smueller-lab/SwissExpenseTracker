from __future__ import annotations

from typing import Any

from dash import dcc
from dash import html

from swiss_exp_tracker.app.components.cards import make_figure_card_MonthYear
from swiss_exp_tracker.app.components.cards import make_number_card
from swiss_exp_tracker.app.components.cards import make_table_card
from swiss_exp_tracker.app.config import config
from swiss_exp_tracker.app.dash_components import GRAPH_CONFIG
from swiss_exp_tracker.app.dash_components import make_card_title
from swiss_exp_tracker.app.dash_components import make_empty_figure
from swiss_exp_tracker.app.dash_components import make_page_title
from swiss_exp_tracker.app.libs import get_heightFigure
from swiss_exp_tracker.app.vis.figure import Fig

F = Fig()
cfg = config()


def layout(data: Any) -> Any:
    donut_all = data.pdf_RetailDonut[data.pdf_RetailDonut["Year"] == "All"]
    total_spend = float(donut_all["amount_CHF"].sum())

    n_years = int(
        data.pdf_RetailDonut[data.pdf_RetailDonut["Year"] != "All"]["Year"].nunique()
    )
    avg_yearly_spend = total_spend / n_years if n_years > 0 else 0

    n_transactions = int(
        (
            (data.pdf_Master["transaction_type"] == "EXPENSE")
            & (data.pdf_Master["category_main"] == "Retail")
        ).sum()
    )
    avg_spend = total_spend / n_transactions if n_transactions > 0 else 0
    avg_transactions_per_year = n_transactions / n_years if n_years > 0 else 0

    s_top_col = [
        {"name": col, "id": col, "type": "text" if col != "amount_CHF" else "numeric"}
        for col in data.pdf_RetailTop.columns
    ]

    s_year = sorted(
        data.pdf_RetailDonut[data.pdf_RetailDonut["Year"] != "All"]["Year"]
        .unique()
        .tolist(),
        reverse=True,
    )

    return html.Div(
        [
            make_page_title("🛍️ Retail Analytics"),
            html.Div(
                [
                    make_number_card(
                        "Avg Yearly Retail Spend", avg_yearly_spend, width=4
                    ),
                    make_number_card("Avg per Entry", avg_spend, width=4),
                    make_number_card(
                        "Avg Transactions / Year",
                        avg_transactions_per_year,
                        width=4,
                        unit="",
                    ),
                    make_figure_card_MonthYear(
                        "Retail spend by subcategory [CHF]",
                        "fig-Retail",
                        width=12,
                        height=get_heightFigure(cfg.npixel_Retail, F.vk_Margin),
                    ),
                    html.Div(
                        [
                            html.Div(
                                className="card-header",
                                children=[
                                    make_card_title("Spend distribution"),
                                    dcc.Dropdown(
                                        id="dropdown-Retail-Year",
                                        className="dropdown-year",
                                        options=[  # type: ignore[arg-type]
                                            {"label": "All", "value": "All"},
                                            *[{"label": y, "value": y} for y in s_year],
                                        ],
                                        value="All",
                                        clearable=False,
                                    ),
                                ],
                            ),
                            dcc.Graph(
                                id="fig-Retail-Donut",
                                className="graph-flex",
                                figure=make_empty_figure(),
                                config=GRAPH_CONFIG,
                            ),
                        ],
                        className="card card-graph col-5",
                    ),
                    make_table_card(
                        title="Top 10 Retail Purchases",
                        s_col=s_top_col,
                        data=data.pdf_RetailTop.to_dict("records"),
                        width=7,
                    ),
                ],
                className="grid",
            ),
        ]
    )
