from dash import Input, Output
from src.analyses.flash_loan_frequency import analyze_flash_loan_frequency
from src.analyses.flash_loan_volume import analyze_flash_loan_volume
from src.analyses.flash_loan_fee import analyze_flash_loan_fee
from src.analyses.transaction_sequence import analyze_transaction_sequence
from src.utils.visualization import plot_frequency, plot_volume, plot_fee, plot_sequence


def register_callbacks(app):
    @app.callback(
        [Output("frequency-graph", "figure"),
         Output("volume-graph", "figure"),
         Output("fee-graph", "figure"),
         Output("sequence-graph", "figure")],
        [Input("network-dropdown", "value"),
         Input("version-dropdown", "value")]
    )
    def update_graphs(network, version):
        frequency_data = analyze_flash_loan_frequency(network, version)
        volume_data = analyze_flash_loan_volume(network, version)
        fee_data = analyze_flash_loan_fee(network, version)
        sequence_data = analyze_transaction_sequence(network, version)

        frequency_fig = plot_frequency(frequency_data)
        volume_fig = plot_volume(volume_data)
        fee_fig = plot_fee(fee_data)
        sequence_fig = plot_sequence(sequence_data)

        return frequency_fig, volume_fig, fee_fig, sequence_fig
