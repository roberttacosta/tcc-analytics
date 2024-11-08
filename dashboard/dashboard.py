from dash import Dash, html, dcc
from dash.dependencies import Input, Output

from src.analyses.flash_loan_frequency import analyze_flash_loan_frequency, group_by_day_hour
from src.utils.visualization import plot_flash_loan_tokens, plot_flash_loan_frequency, plot_day_hour_distribution, \
    plot_flash_loan_volume, plot_flash_loan_volume_all, plot_wallet_interactions
from src.analyses.flash_loan_tokens import analyze_flash_loan_tokens
from src.analyses.flash_loan_volume import analyze_flash_loan_volume, analyze_flash_loan_volume_all
from src.analyses.transaction_sequence import analyze_flash_loan_wallets
import logging

app = Dash(__name__)
app.config.suppress_callback_exceptions = True

app.layout = html.Div([
    html.H1("Dashboard de Análise de Flash Loans - Aave"),
    html.Div([
        html.H2("Frequência dos Flash Loans"),
        dcc.Graph(id="frequency-plot"),
        dcc.Checklist(
            id='network-separation',
            options=[{'label': 'Separar por Rede', 'value': 'separate'}],
            value=['separate']
        )
    ]),
    html.Div([
        html.H2("Distribuição por Dia e Horário"),
        dcc.Graph(id="day-hour-distribution-plot")
    ]),
    html.Div([
        html.H2("Top Tokens Utilizados em Flash Loans"),
        dcc.Graph(id="tokens-plot"),
        dcc.Checklist(
            id='network-separation-tokens',
            options=[{'label': 'Separar por Rede', 'value': 'separate'}],
            value=['separate']
        )
    ]),
    html.Div([
        html.H2("Volume de Transações Bem-Sucedidas"),
        dcc.Graph(id="volume-plot"),
        dcc.Checklist(
            id='network-separation-volume',
            options=[{'label': 'Separar por Rede', 'value': 'separate'}],
            value=['separate']
        )
    ]),
    html.Div([
        html.H2("Volume de Todas as Transações"),
        dcc.Graph(id="volume-all-plot"),
        dcc.Checklist(
            id='network-separation-volume-all',
            options=[{'label': 'Separar por Rede', 'value': 'separate'}],
            value=['separate']
        )
    ]),
    html.Div([
        html.H2("Tipos de Interação nas 5 Transações Subsequentes - Polygon"),
        dcc.Graph(id="wallet-interactions-plot-polygon")
    ]),
    html.Div([
        html.H2("Tipos de Interação nas 5 Transações Subsequentes - Ethereum"),
        dcc.Graph(id="wallet-interactions-plot-ethereum")
    ]),
    dcc.Interval(
        id="interval-component",
        interval=60 * 1000,  # Atualiza a cada 60 segundos
        n_intervals=0
    )
])


@app.callback(
    Output("frequency-plot", "figure"),
    Input("interval-component", "n_intervals"),
    Input("network-separation", "value")
)
def update_frequency_plot(n_intervals, network_separation):
    separate_by_network = 'separate' in network_separation
    frequency_data = analyze_flash_loan_frequency(separate_by_network=separate_by_network)
    frequency_plot = plot_flash_loan_frequency(frequency_data, separate_by_network)
    return frequency_plot


@app.callback(
    Output("day-hour-distribution-plot", "figure"),
    Input("interval-component", "n_intervals")
)
def update_day_hour_distribution_plot(n_intervals):
    pivot_data = group_by_day_hour()
    fig = plot_day_hour_distribution(pivot_data)
    return fig


@app.callback(
    Output("tokens-plot", "figure"),
    Input("interval-component", "n_intervals"),
    Input("network-separation-tokens", "value")
)
def update_tokens_plot(n_intervals, network_separation_tokens):
    separate_by_network = 'separate' in network_separation_tokens
    token_data = analyze_flash_loan_tokens(separate_by_network=separate_by_network)
    tokens_plot = plot_flash_loan_tokens(token_data, separate_by_network)
    return tokens_plot


@app.callback(
    Output("volume-plot", "figure"),
    Input("interval-component", "n_intervals"),
    Input("network-separation-volume", "value")
)
def update_volume_plot(n_intervals, network_separation_volume):
    separate_by_network = 'separate' in network_separation_volume
    volume_data = analyze_flash_loan_volume()
    volume_plot = plot_flash_loan_volume(volume_data, separate_by_network)
    return volume_plot


@app.callback(
    Output("volume-all-plot", "figure"),
    Input("interval-component", "n_intervals"),
    Input("network-separation-volume-all", "value")
)
def update_volume_all_plot(n_intervals, network_separation_volume_all):
    separate_by_network = 'separate' in network_separation_volume_all
    volume_data = analyze_flash_loan_volume_all(separate_by_network=separate_by_network)
    volume_plot = plot_flash_loan_volume_all(volume_data, separate_by_network)
    return volume_plot


@app.callback(
    Output("wallet-interactions-plot-polygon", "figure"),
    Output("wallet-interactions-plot-ethereum", "figure"),
    Input("interval-component", "n_intervals")
)
def update_wallet_interactions_plot(n_intervals):
    flash_loan_wallets_analysis_polygon, flash_loan_wallets_analysis_ethereum = analyze_flash_loan_wallets()
    wallet_interactions_plot_polygon = plot_wallet_interactions(flash_loan_wallets_analysis_polygon, 'polygon')
    wallet_interactions_plot_ethereum = plot_wallet_interactions(flash_loan_wallets_analysis_ethereum, 'ethereum')
    return wallet_interactions_plot_polygon, wallet_interactions_plot_ethereum


if __name__ == "__main__":
    logging.info("Iniciando o servidor do Dash...")
    app.run_server(debug=True)
