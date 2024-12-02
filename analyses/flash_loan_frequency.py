import pandas as pd
from src.data.data_loader import load_all_transactions
from src.utils.helpers import get_from_cache, save_to_cache
import logging
import json


def analyze_flash_loan_frequency(use_cache=True, separate_by_network=True):
    cache_key = 'flash_loan_frequency'

    if use_cache:
        cached_data = get_from_cache(cache_key)
        if cached_data is not None and not cached_data.empty:
            logging.info("Dados carregados do cache Redis.")
            return cached_data

    # Carrega todos os dados das transações
    flash_loans = load_all_transactions(function_name=['flashLoan', 'flashLoanSimple'])

    # Log the columns of the DataFrame
    logging.info(f"Columns in flash_loans DataFrame: {flash_loans.columns}")

    # Check if 'timestamp' column exists
    if 'timestamp' not in flash_loans.columns:
        logging.error("Column 'timestamp' not found in the DataFrame.")
        return None

    flash_loans['timestamp'] = pd.to_datetime(flash_loans['timestamp'], unit='s')

    # Filtra as transações pelas funções desejadas
    flash_loans = flash_loans[flash_loans['function_name'].isin(['flashLoan', 'flashLoanSimple'])]

    if separate_by_network:
        # Agrupa por data e rede para calcular a frequência
        frequency_data = flash_loans.groupby([flash_loans['timestamp'].dt.date, 'network']).size().reset_index(
            name='count')
    else:
        # Agrupa apenas por data para calcular a frequência
        frequency_data = flash_loans.groupby(flash_loans['timestamp'].dt.date).size().reset_index(name='count')

    # Log the frequency data (first 5 rows)
    logging.info(f"Frequency data (first 5 rows):\n{frequency_data.head()}")

    save_to_cache(cache_key, frequency_data)
    logging.info(f"Dados salvos no cache Redis com a chave: {cache_key}")

    return frequency_data


def extract_day_hour(use_cache=True):
    cache_key_polygon = 'flash_loan_frequency_day_hour_polygon'
    cache_key_ethereum = 'flash_loan_frequency_day_hour_ethereum'

    if use_cache:
        cached_data_polygon = get_from_cache(cache_key_polygon)
        cached_data_ethereum = get_from_cache(cache_key_ethereum)

        if cached_data_polygon is not None and cached_data_ethereum is not None:
            logging.info("Dados carregados do cache Redis para Polygon e Ethereum.")
            frequency_data_polygon = pd.read_json(cached_data_polygon)
            frequency_data_ethereum = pd.read_json(cached_data_ethereum)
            return frequency_data_polygon, frequency_data_ethereum

    # Carrega todos os dados das transações
    flash_loans = load_all_transactions(function_name=['flashLoan', 'flashLoanSimple'])

    # Log the columns of the DataFrame
    logging.info(f"Columns in flash_loans DataFrame: {flash_loans.columns}")

    # Check if 'timestamp' and 'network' columns exist
    if 'timestamp' not in flash_loans.columns or 'network' not in flash_loans.columns:
        logging.error("Column 'timestamp' or 'network' not found in the DataFrame.")
        return None, None

    flash_loans['timestamp'] = pd.to_datetime(flash_loans['timestamp'], unit='s')

    frequency_data = flash_loans[flash_loans['function_name'].isin(['flashLoan', 'flashLoanSimple'])]

    frequency_data['timestamp'] = pd.to_datetime(frequency_data['timestamp'])
    frequency_data['day_of_week'] = frequency_data['timestamp'].dt.day_name()
    frequency_data['hour'] = frequency_data['timestamp'].dt.floor('30T').dt.hour

    # Agrupar por rede (network), dia da semana e hora arredondada, contando as ocorrências
    grouped_data = frequency_data.groupby(['network', 'day_of_week', 'hour']).size().reset_index(name='count')

    # Dividir os dados por rede
    polygon_data = grouped_data[grouped_data['network'] == 'polygon']
    ethereum_data = grouped_data[grouped_data['network'] == 'ethereum']

    # Converte os DataFrames para JSON
    polygon_data_json = polygon_data.to_json(orient='records')
    ethereum_data_json = ethereum_data.to_json(orient='records')

    # Salva os JSONs no cache
    save_to_cache(cache_key_polygon, polygon_data_json)
    save_to_cache(cache_key_ethereum, ethereum_data_json)
    logging.info(f"Dados salvos no cache Redis com as chaves: {cache_key_polygon} e {cache_key_ethereum}")

    return polygon_data, ethereum_data


def group_by_day_hour():
    frequency_data_polygon, frequency_data_ethereum = extract_day_hour()

    # Mapping of English day names to Portuguese
    day_name_mapping = {
        'Monday': 'Segunda-feira',
        'Tuesday': 'Terça-feira',
        'Wednesday': 'Quarta-feira',
        'Thursday': 'Quinta-feira',
        'Friday': 'Sexta-feira',
        'Saturday': 'Sábado',
        'Sunday': 'Domingo'
    }

    # Função auxiliar para preparar os dados
    def prepare_frequency_data(frequency_data):
        frequency_data['day_of_week'] = frequency_data['day_of_week'].map(day_name_mapping)
        days_of_week_order = ['Sábado', 'Sexta-feira', 'Quinta-feira', 'Quarta-feira', 'Terça-feira', 'Segunda-feira', 'Domingo']
        frequency_data['day_of_week'] = pd.Categorical(frequency_data['day_of_week'], categories=days_of_week_order, ordered=True)
        frequency_data = frequency_data.sort_values(['day_of_week', 'hour'])
        pivot_data = frequency_data.pivot_table(index='day_of_week', columns='hour', values='count', fill_value=0)
        pivot_data = pivot_data.reindex(columns=range(24), fill_value=0)
        return pivot_data

    pivot_data_polygon = prepare_frequency_data(frequency_data_polygon)
    pivot_data_ethereum = prepare_frequency_data(frequency_data_ethereum)

    return pivot_data_polygon, pivot_data_ethereum
