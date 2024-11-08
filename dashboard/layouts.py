from dash import dcc, html


def create_layout():
    return html.Div([
        html.H1("Dashboard de Análise de Flash Loans"),
        dcc.Dropdown(
            id="network-dropdown",
            options=[
                {"label": "Ethereum", "value": "ethereum"},
                {"label": "Polygon", "value": "polygon"}
            ],
            value="ethereum",
            placeholder="Selecione a rede"
        ),
        dcc.Dropdown(
            id="version-dropdown",
            options=[
                {"label": "v1", "value": "v1"},
                {"label": "v2", "value": "v2"},
                {"label": "v3", "value": "v3"}
            ],
            value="v1",
            placeholder="Selecione a versão"
        ),
        html.Div(id="graphs-container", children=[
            dcc.Graph(id="frequency-graph"),
            dcc.Graph(id="volume-graph"),
            dcc.Graph(id="fee-graph"),
            dcc.Graph(id="sequence-graph"),
        ]),
    ])
