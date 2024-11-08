import plotly.express as px
import pandas as pd
import plotly.graph_objects as go


def plot_flash_loan_frequency(frequency_data, separate_by_network):
    color_discrete_map = {'polygon': 'purple', 'ethereum': 'cyan'}
    if separate_by_network:
        fig = px.line(frequency_data, x='timestamp', y='count', color='network',
                      title='Frequência de Flash Loans por Rede', color_discrete_map=color_discrete_map)
    else:
        fig = px.line(frequency_data, x='timestamp', y='count', title='Frequência de Flash Loans')
    return fig


def plot_day_hour_distribution(pivot_data):
    # Formatar as horas
    hour_labels = [f"{hour:02d}:00" for hour in range(24)]

    fig = go.Figure(data=go.Heatmap(
        z=pivot_data.values,
        x=hour_labels,
        y=pivot_data.index,
        colorscale='YlOrBr',
        text=pivot_data.values,
        texttemplate="%{text}",
        textfont={"size": 12}
    ))

    fig.update_layout(
        title="Distribuição de Flash Loans por Dia e Hora",
        xaxis=dict(title="Hora", tickmode='array', tickvals=hour_labels),
        yaxis=dict(title="Dia da Semana")
    )

    return fig


def plot_flash_loan_tokens(token_data, separate_by_network=True):
    color_discrete_map = {'polygon': 'purple', 'ethereum': 'cyan'}
    if separate_by_network:
        fig = px.bar(token_data, x='count', y='token', color='network',
                     title='Top Tokens Utilizados em Flash Loans por Rede',
                     orientation='h',
                     category_orders={'token': token_data.sort_values('count', ascending=False)['token']},
                     text='count', color_discrete_map=color_discrete_map)
    else:
        fig = px.bar(token_data, x='count', y='token',
                     title='Top Tokens Utilizados em Flash Loans',
                     orientation='h',
                     category_orders={'token': token_data.sort_values('count', ascending=False)['token']},
                     text='count')

    fig.update_traces(marker=dict(line=dict(width=3)), textposition='outside', width=1.0)
    fig.update_layout(yaxis=dict(tickmode='linear', dtick=1), bargap=0.8, height=1200,
                      legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.90, itemwidth=30))
    return fig


def plot_flash_loan_volume(volume_data, separate_by_network=False):
    if volume_data is None or volume_data.empty:
        print("No data available to plot.")
        return None

    # Agrupando os dados e somando os valores
    if separate_by_network and 'network' in volume_data.columns:
        volume_data = volume_data.groupby(['function_name', 'network']).agg({'count': 'sum'}).reset_index()
    else:
        volume_data = volume_data.groupby(['function_name']).agg({'count': 'sum'}).reset_index()
        volume_data['network'] = 'Total'  # Adiciona uma coluna para representar a ausência de rede

    # Ordenar os dados pelo volume de transações, de forma decrescente
    volume_data = volume_data.sort_values(by='count', ascending=False)

    # Criando o gráfico de barras horizontais agrupadas com escala logarítmica
    fig = px.bar(volume_data, y='function_name', x='count', color='network',
                 title="Volume de Transações por Função e Rede (Escala Logarítmica)",
                 barmode='group',  # Agrupa as barras lado a lado
                 text='count',  # Mostra o valor ao lado de cada barra
                 color_discrete_map={'polygon': 'purple', 'ethereum': 'cyan', 'Total': 'gray'},
                 log_x=True)  # Aplica a escala logarítmica no eixo X

    # Ajuste do layout e aparência do gráfico
    fig.update_traces(
        marker=dict(line=dict(width=1), opacity=0.8),
        textposition='outside',
        width=0.35,
        texttemplate='%{text:.0f}'.replace(".", ",")  # Exibe o texto dos valores com pontos para milhares
    )
    fig.update_layout(
        xaxis_title="",
        yaxis_title="Function Name",
        xaxis=dict(showticklabels=False),  # Remove os rótulos do eixo X
        yaxis=dict(
            categoryorder='total ascending',  # Ordenação das funções
            automargin=True,
            side="left"
        ),
        height=1400,
        legend=dict(
            orientation="v",
            yanchor="bottom",
            y=0.01,
            xanchor="right",
            x=0.98,
            bgcolor="rgba(255, 255, 255, 0.8)"
        ),
        margin=dict(l=200),  # Aumenta a margem esquerda para dar espaço aos rótulos alinhados à esquerda
        bargap=0.2  # Aumenta o espaço entre as barras
    )

    # Atualiza os valores de texto para o padrão brasileiro (com pontos para separar milhares)
    fig.for_each_trace(lambda t: t.update(text=[f"{int(v):,}".replace(",", ".") for v in t.x]))

    return fig


def plot_flash_loan_volume_all(volume_data, separate_by_network=True):
    if volume_data is None or volume_data.empty:
        print("No data available to plot.")
        return None

    # Ensure 'count' is numeric
    volume_data['count'] = volume_data['count'].apply(lambda x: int(x))

    # Calcular o total de transações por rede
    total_by_network = volume_data.groupby('network')['count'].sum().reset_index()
    total_by_network = total_by_network.rename(columns={'count': 'total_count'})

    # Mesclar os dados para obter o total de transações por rede
    volume_data = volume_data.merge(total_by_network, on='network')

    # Calcular o percentual
    volume_data['percent'] = (volume_data['count'] / volume_data['total_count']) * 100

    # Adicionar dados de 'all' para representar 100%
    all_data = total_by_network.copy()
    all_data['type'] = 'all'
    all_data['percent'] = 100

    # Filtrar apenas os dados de flashLoan
    flash_loan_data = volume_data[volume_data['type'] == 'flashLoan']

    # Combinar os dados
    combined_data = pd.concat([flash_loan_data, all_data])

    # Create the figure
    fig = go.Figure()

    # Add the 'all' bars
    fig.add_trace(go.Bar(
        y=combined_data[combined_data['type'] == 'all']['network'],
        x=combined_data[combined_data['type'] == 'all']['percent'],
        name='All Transactions',
        orientation='h',
        marker=dict(color='cyan'),
        width=0.3,
        text=[f"{100 - percent:.2f}%" for percent in combined_data[combined_data['type'] == 'flashLoan']['percent']]
    ))

    # Add the 'flashLoan' bars
    fig.add_trace(go.Bar(
        y=combined_data[combined_data['type'] == 'flashLoan']['network'],
        x=combined_data[combined_data['type'] == 'flashLoan']['percent'],
        name='Flash Loan',
        orientation='h',
        marker=dict(color='purple'),
        width=0.3,
        text=[f"{percent:.2f}%" for percent in combined_data[combined_data['type'] == 'flashLoan']['percent']]
    ))

    # Update layout
    fig.update_layout(
        title='Percentual de Transações por Rede',
        barmode='overlay',
        height=400,
        legend=dict(yanchor="bottom", y=0.01, xanchor="right", x=0.98, itemwidth=30),
        yaxis=dict(tickmode='linear', dtick=1, title='Rede'),
        xaxis=dict(title='Percentual', tickformat='.2f', range=[0, 120], showticklabels=False)
    )

    return fig


def plot_wallet_interactions(transactions_data, network):
    # Convert the transactions data to a DataFrame if it's not already
    if not isinstance(transactions_data, pd.DataFrame):
        transactions_data = pd.DataFrame(transactions_data)

    # Ensure 'function_name' is a categorical type with 'flashLoan' and 'flashLoanSimple' first
    transactions_data['function_name'] = pd.Categorical(
        transactions_data['function_name'],
        categories=['flashLoan', 'flashLoanSimple'] + [x for x in transactions_data['function_name'].unique() if
                                                       x not in ['flashLoan', 'flashLoanSimple']],
        ordered=True
    )

    # Remove rows with null values in 'wallet' or 'function_name'
    transactions_data = transactions_data.dropna(subset=['wallet', 'function_name'])

    # Sort transactions so that 'flashLoan' and 'flashLoanSimple' are at the beginning
    transactions_data = transactions_data.sort_values(by=['wallet', 'function_name'])

    # Add a column to indicate the order of transactions
    transactions_data['order'] = transactions_data.groupby('wallet').cumcount() + 1

    # Filter to keep only the first 6 transactions for each wallet
    transactions_data = transactions_data[transactions_data['order'] <= 6]

    # Count the types of interactions
    interaction_counts = transactions_data.groupby(['wallet', 'function_name']).size().reset_index(name='count')

    # Create the stacked bar chart
    fig = px.bar(interaction_counts, x='wallet', y='count', color='function_name',
                 title=f'Tipos de Interação nas 6 Transações Subsequentes - {network.capitalize()}')

    # Update layout to standardize bar sizes and set y-axis range
    fig.update_layout(barmode='stack', uniformtext_minsize=8, uniformtext_mode='hide',
                      yaxis=dict(range=[0, 6], dtick=1))
    fig.update_traces(marker=dict(line=dict(width=0.5)), textposition='inside')

    return fig
