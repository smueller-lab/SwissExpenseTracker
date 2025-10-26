import plotly.io as pio

myTemp = {
    "layout": {
        "template": "plotly_dark",
        "paper_bgcolor": "#12263A",
        "plot_bgcolor": "#12263A",
        "font": {
            "color": "white",
            "family": "Arial",
            "size": 14
        },
        "title": {
            "font": {"size": 20, "color": "white"}
        },
        "margin": {"l": 40, "r": 40, "t": 20, "b": 40},
        "xaxis": {
            "showgrid": False,
            "zeroline": False,
            "showline": True,
            "linewidth": 1,
            "linecolor": "white",
            "mirror": True,
            "ticks": "inside",
            "ticklen": 5,
            "tickwidth": 1,
            "tickcolor": "white"
        },
        "yaxis": {
            "showgrid": True,
            "gridcolor": "rgba(255, 255, 255, 0.05)",
            "gridwidth": 1,
            "zeroline": False,
            "showline": True,
            "linewidth": 1,
            "linecolor": "white",
            "mirror": True,
            "ticks": "inside",
            "ticklen": 5,
            "tickwidth": 1,
            "tickcolor": "white"
        }
    },
    "data": {
        "scatter": [
            {
                "marker": {
                    "size": 8,
                    "color": "#19D3F3",
                    "line": {"color": "white", "width": 0.5}
                }
            }
        ]
    }   
}

pio.templates['myTemp'] = myTemp