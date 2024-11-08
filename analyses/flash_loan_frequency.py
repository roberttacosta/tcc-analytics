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
    cache_key = 'flash_loan_frequency_day_hour'

    if use_cache:
        cached_data = get_from_cache(cache_key)
        if cached_data is not None and cached_data:
            logging.info("Dados carregados do cache Redis.")
            frequency_data = pd.read_json(cached_data)
            return frequency_data

    # Carrega todos os dados das transações
    flash_loans = load_all_transactions(function_name=['flashLoan', 'flashLoanSimple'])

    # Log the columns of the DataFrame
    logging.info(f"Columns in flash_loans DataFrame: {flash_loans.columns}")

    # Check if 'timestamp' column exists
    if 'timestamp' not in flash_loans.columns:
        logging.error("Column 'timestamp' not found in the DataFrame.")
        return None

    flash_loans['timestamp'] = pd.to_datetime(flash_loans['timestamp'], unit='s')

    frequency_data = flash_loans[flash_loans['function_name'].isin(['flashLoan', 'flashLoanSimple'])]

    frequency_data['timestamp'] = pd.to_datetime(frequency_data['timestamp'])
    frequency_data['day_of_week'] = frequency_data['timestamp'].dt.day_name()
    frequency_data['hour'] = frequency_data['timestamp'].dt.floor('30T').dt.hour

    # Agrupar por dia da semana e hora arredondada, contando as ocorrências
    grouped_data = frequency_data.groupby(['day_of_week', 'hour']).size().reset_index(name='count')

    # Log the grouped data (first 5 rows)
    logging.info(f"Grouped data (first 5 rows):\n{grouped_data.head()}")

    # Converte o DataFrame para JSON
    grouped_data_json = grouped_data.to_json(orient='records')

    # Salva o JSON no cache
    save_to_cache(cache_key, grouped_data_json)
    logging.info(f"Dados salvos no cache Redis com a chave: {cache_key}")

    return grouped_data


def group_by_day_hour():
    frequency_data = extract_day_hour()

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

    # Apply the mapping to the day_of_week column
    frequency_data['day_of_week'] = frequency_data['day_of_week'].map(day_name_mapping)

    # Order the days of the week in Portuguese starting from Sunday
    days_of_week_order = ['Sábado', 'Sexta-feira', 'Quinta-feira', 'Quarta-feira', 'Terça-feira', 'Segunda-feira', 'Domingo']
    frequency_data['day_of_week'] = pd.Categorical(frequency_data['day_of_week'], categories=days_of_week_order, ordered=True)
    frequency_data = frequency_data.sort_values(['day_of_week', 'hour'])

    # Pivot the data to create a table with hours as columns and days of the week as indices
    pivot_data = frequency_data.pivot_table(index='day_of_week', columns='hour', values='count', fill_value=0)

    # Ensure all hours from 0 to 23 are present as columns
    pivot_data = pivot_data.reindex(columns=range(24), fill_value=0)

    return pivot_data
