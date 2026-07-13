from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go  # pyright: ignore[reportMissingTypeStubs]
import plotly.io as pio  # pyright: ignore[reportMissingTypeStubs]

import swiss_exp_tracker.app.vis.ploty_template  # pyright: ignore[reportUnusedImport] # noqa: F401

pio.templates.default = "myTemp"

_COLORS = [
    "#19D3F3",
    "#E38A04",
    "#2ECC71",
    "#9B59B6",
    "#E74C3C",
    "#F39C12",
    "#1ABC9C",
    "#E67E22",
    "#45C7F6",
    "#FAF263",
]


def _make_no_data_fig(label: str) -> go.Figure:
    """Return a styled placeholder figure when data is unavailable."""
    return go.Figure(
        layout=go.Layout(
            paper_bgcolor="#12263A",
            plot_bgcolor="#12263A",
            annotations=[
                go.layout.Annotation(
                    text=label,
                    xref="paper",
                    yref="paper",
                    x=0.5,
                    y=0.5,
                    showarrow=False,
                    font={"color": "white", "size": 16},
                )
            ],
            xaxis={"visible": False},
            yaxis={"visible": False},
        )
    )


def fig_allocation_donut(pdf_latest: pd.DataFrame) -> go.Figure:
    if pdf_latest.empty:
        return _make_no_data_fig(
            "No Swissquote data found. Import your positions data first."
        )
    total = pdf_latest["value_chf"].sum()
    pct = (
        pdf_latest["value_chf"] / total * 100
        if total > 0
        else pdf_latest["value_chf"] * 0
    )
    text = [
        f"{sym}<br>{p:.1f}%<br>CHF {v:,.0f}" if p >= 3 else ""
        for sym, p, v in zip(
            pdf_latest["symbol"], pct, pdf_latest["value_chf"], strict=True
        )
    ]
    names = pdf_latest["name"].fillna(pdf_latest["symbol"]).tolist()
    fig = go.Figure(
        go.Pie(
            labels=pdf_latest["symbol"],
            values=pdf_latest["value_chf"],
            hole=0.4,
            text=text,
            customdata=names,
            textinfo="text",
            textfont={"size": 12},
            pull=[0.02] * len(pdf_latest),
            domain={"x": [0.0, 0.9], "y": [0.0, 1.0]},
            marker={"colors": _COLORS[: len(pdf_latest)]},
            hovertemplate=(
                "<b>%{label}</b><br>%{customdata}<br>"
                "%{value:,.0f} CHF (%{percent})<extra></extra>"
            ),
        )
    )
    fig.update_layout(showlegend=False, height=420)
    return fig


def fig_portfolio_progression(pdf: pd.DataFrame) -> go.Figure:
    if pdf.empty:
        return _make_no_data_fig(
            "No Swissquote data found. Import your positions data first."
        )
    pdf_agg = pdf.groupby("date")[["value_chf", "invested_chf"]].sum().reset_index()

    fig = go.Figure()
    # Invested drawn first so fill on value trace goes from value down to this line
    fig.add_trace(
        go.Scatter(
            x=pdf_agg["date"],
            y=pdf_agg["invested_chf"],
            mode="lines+markers",
            name="Invested",
            line={"width": 2, "dash": "dash", "color": "#95A5A6"},
            marker={"size": 5, "color": "#95A5A6"},
        )
    )
    fig.add_trace(
        go.Scatter(
            x=pdf_agg["date"],
            y=pdf_agg["value_chf"],
            mode="lines+markers",
            name="Portfolio Value",
            line={"width": 2, "color": "#19D3F3"},
            marker={"size": 5, "color": "#19D3F3"},
            fill="tonexty",
            fillcolor="rgba(25, 211, 243, 0.15)",
        )
    )
    fig.update_layout(
        yaxis={"showline": True, "linecolor": "white", "tickprefix": "CHF "},
        xaxis={"showline": True, "linecolor": "white", "tickformat": "%Y-%m-%d"},
        height=350,
        margin={"l": 80},
        legend={
            "orientation": "v",
            "yanchor": "middle",
            "y": 0.5,
            "xanchor": "left",
            "x": 1.02,
        },
    )
    return fig


def fig_position_value(pdf: pd.DataFrame, symbols: list[str]) -> go.Figure:
    if pdf.empty or not symbols:
        return _make_no_data_fig("No position data available for the selected symbols.")
    fig = go.Figure()
    pdf_filtered = pdf[pdf["symbol"].isin(symbols)]
    for i, symbol in enumerate(symbols):
        grp = pdf_filtered[pdf_filtered["symbol"] == symbol].sort_values("date")
        name = (
            grp["name"].dropna().iloc[0]
            if "name" in grp.columns and not grp["name"].dropna().empty
            else symbol
        )
        fig.add_trace(
            go.Scatter(
                x=grp["date"],
                y=grp["value_chf"],
                mode="lines+markers",
                name=symbol,
                line={"color": _COLORS[i % len(_COLORS)]},
                marker={"size": 5, "color": _COLORS[i % len(_COLORS)]},
                hovertemplate=(
                    f"<b>{symbol}</b> — {name}<br>"
                    "%{x|%Y-%m-%d}<br>CHF %{y:,.0f}<extra></extra>"
                ),
            )
        )
    fig.update_layout(
        yaxis={"showline": True, "linecolor": "white", "tickprefix": "CHF "},
        xaxis={"showline": True, "linecolor": "white", "tickformat": "%Y-%m-%d"},
        autosize=True,
        uirevision="pos-value",
        margin={"l": 80},
        legend={
            "orientation": "v",
            "yanchor": "middle",
            "y": 0.5,
            "xanchor": "left",
            "x": 1.02,
        },
    )
    return fig


def fig_position_pct(pdf: pd.DataFrame, symbols: list[str]) -> go.Figure:
    if pdf.empty or not symbols:
        return _make_no_data_fig("No position data available for the selected symbols.")
    fig = go.Figure()
    pdf_filtered = pdf[pdf["symbol"].isin(symbols)]
    for i, symbol in enumerate(symbols):
        grp = pdf_filtered[pdf_filtered["symbol"] == symbol].sort_values("date")
        name = (
            grp["name"].dropna().iloc[0]
            if "name" in grp.columns and not grp["name"].dropna().empty
            else symbol
        )
        fig.add_trace(
            go.Scatter(
                x=grp["date"],
                y=grp["pnl_pct"] * 100,
                mode="lines+markers",
                name=symbol,
                line={"color": _COLORS[i % len(_COLORS)]},
                marker={"size": 5, "color": _COLORS[i % len(_COLORS)]},
                hovertemplate=(
                    f"<b>{symbol}</b> — {name}<br>"
                    "%{x|%Y-%m-%d}<br>%{y:+.2f} %<extra></extra>"
                ),
            )
        )
    fig.add_hline(
        y=0,
        line={"color": "rgba(255,255,255,0.3)", "dash": "dot", "width": 1},
    )
    fig.update_layout(
        yaxis={"showline": True, "linecolor": "white", "ticksuffix": " %"},
        xaxis={"showline": True, "linecolor": "white", "tickformat": "%Y-%m-%d"},
        autosize=True,
        uirevision="pos-pct",
        margin={"l": 65},
        legend={
            "orientation": "v",
            "yanchor": "middle",
            "y": 0.5,
            "xanchor": "left",
            "x": 1.02,
        },
    )
    return fig
