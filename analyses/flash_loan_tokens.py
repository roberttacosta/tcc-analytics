import pandas as pd
from collections import Counter
from src.data.data_loader import load_all_transactions
from src.utils.helpers import get_from_cache, save_to_cache
import logging


def parse_flashLoanSimple_input(input_data):
    # Remover o prefixo '0x' se estiver presente
    if input_data.startswith("0x"):
        input_data = input_data[2:]

    # Extrair o method_id (primeiros 4 bytes = 8 caracteres hexadecimais)
    method_id = input_data[:8]

    # Os parâmetros começam após o method_id
    parameters = [input_data[i:i + 64] for i in range(8, len(input_data), 64)]

    # Mapear os parâmetros conforme a assinatura
    asset = "0x" + parameters[1][-40:]  # Este é o token usado

    return asset


def analyze_flash_loan_tokens(use_cache=True, separate_by_network=True):
    cache_key = 'flash_loan_tokens'

    if use_cache:
        cached_data = get_from_cache(cache_key)
        if cached_data is not None and not cached_data.empty:
            return cached_data

    flash_loans = load_all_transactions(function_name=['flashLoan', 'flashLoanSimple'])
    flash_loans = flash_loans[flash_loans['is_error'] == 0]  # Filtrar transações sem erro
    flash_loans['timestamp'] = pd.to_datetime(flash_loans['timestamp'])

    # Extrair tokens do input
    flash_loans['token'] = flash_loans['input'].apply(parse_flashLoanSimple_input)

    # Filtrar tokens inválidos
    invalid_tokens = {'0x' + '0' * 40, '0x00000000000000000000000000000000000000e0'}
    flash_loans = flash_loans[~flash_loans['token'].isin(invalid_tokens)]

    # Contar a frequência dos tokens
    if separate_by_network:
        token_data = flash_loans.groupby(['network', 'token']).size().reset_index(name='count')
    else:
        token_counts = Counter(flash_loans['token'])
        token_data = pd.DataFrame(token_counts.items(), columns=['token', 'count'])

    # Log dos primeiros tokens que serão salvos
    logging.info("Primeiros tokens que serão salvos no Redis:")
    for index, row in token_data.head(5).iterrows():
        logging.info(f"Token: {row['token']}, Count: {row['count']}")

    save_to_cache(cache_key, token_data)
    return token_data
