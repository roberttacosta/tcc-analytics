import pandas as pd
from decimal import Decimal, getcontext
from src.data.data_loader import load_all_transactions
from src.utils.helpers import get_from_cache, save_to_cache
from src.utils.decoder_input import decode_flash_loan_transaction
import logging
import random

# Ajustar a precisão global do Decimal
getcontext().prec = 50

# Dicionário de precisões dos tokens mais comuns
token_decimals = {
    '0x0000000000000000000000000000000000000000': 18,  # Ether
    '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48': 6,  # USDC
    '0xdac17f958d2ee523a2206206994597c13d831ec7': 6,  # USDT
    '0x6b175474e89094c44da98b954eedeac495271d0f': 18,  # DAI
}

# Preços das moedas em 31-10-2024
eth_price_usd = Decimal('2515.87')  # Preço do ETH em dólares
pol_price_usd = Decimal('0.32')  # Preço do MATIC em dólares

# Função para analisar as taxas dos flash loans
def analyze_flash_loan_fee(use_cache=True):
    cache_key_ethereum = 'flash_loan_fee_ethereum'
    cache_key_polygon = 'flash_loan_fee_polygon'

    if use_cache:
        ethereum_cached_data = get_from_cache(cache_key_ethereum)
        polygon_cached_data = get_from_cache(cache_key_polygon)
        if ethereum_cached_data and polygon_cached_data:
            logging.info("Dados carregados do cache Redis.")
            return ethereum_cached_data, polygon_cached_data

    # Carrega todas as transações
    flash_loans = load_all_transactions(function_name=['flashLoan', 'flashLoanSimple'])

    # Converter a lista de transações em um DataFrame do pandas
    df = pd.DataFrame(flash_loans)

    # Verificar se o DataFrame está vazio
    if df.empty:
        logging.warning("Nenhuma transação encontrada.")
        return {}, {}

    # Ajustar o gas_price para o formato correto em Gwei usando Decimal
    df['gas_price'] = df['gas_price'].apply(lambda x: Decimal(x).scaleb(-18))

    # Log gas_price values in Gwei
    logging.info(f"Gas_price values in Gwei: {df['gas_price'].head()}")

    # Calcular a taxa paga (gas_used * gas_price) usando Decimal
    df['fee_paid'] = df.apply(lambda row: Decimal(row['gas_used']) * row['gas_price'], axis=1)

    # Log fee_paid values
    logging.info(f"Fee_paid values: {df['fee_paid'].head()}")

    # Filtrar transações por rede
    networks = ['ethereum', 'polygon']
    metrics = {}

    for network in networks:
        filtered_df = df[df['network'] == network]
        if filtered_df.empty:
            logging.warning(f"Nenhuma transação encontrada para a rede {network}.")
            continue
        if network == 'ethereum':
            network_metrics = calculate_metrics(filtered_df, eth_price_usd)
        elif network == 'polygon':
            network_metrics = calculate_metrics(filtered_df, pol_price_usd)
        metrics[network] = network_metrics

        # Salvar os resultados no cache Redis
        save_to_cache(f'flash_loan_fee_{network}', network_metrics)
        logging.info(f"Dados salvos no cache Redis com a chave: flash_loan_fee_{network}")

    return metrics.get('ethereum', {}), metrics.get('polygon', {})

# Função para calcular montante total e valor médio
def calculate_metrics(filtered_df, price_usd):
    # Garantir que 'fee_paid' seja Decimal
    filtered_df['fee_paid'] = filtered_df['fee_paid'].apply(Decimal)

    total_fee_paid = abs(filtered_df['fee_paid'].sum())
    average_fee_paid = abs(filtered_df['fee_paid'].mean())

    # Certificar que os valores são Decimals antes de fazer cálculos
    total_fee_paid = Decimal(total_fee_paid)
    average_fee_paid = Decimal(average_fee_paid)
    price_usd = Decimal(price_usd)

    # Log dos valores antes da conversão para USD
    logging.info(f"Total fee paid before USD conversion: {total_fee_paid}")
    logging.info(f"Average fee paid before USD conversion: {average_fee_paid}")

    total_fee_paid_usd = total_fee_paid * price_usd
    average_fee_paid_usd = average_fee_paid * price_usd

    # Log dos valores após a conversão para USD
    logging.info(f"Total fee paid in USD: {total_fee_paid_usd}")
    logging.info(f"Average fee paid in USD: {average_fee_paid_usd}")

    return {
        'total_fee_paid': total_fee_paid,
        'average_fee_paid': average_fee_paid,
        'total_fee_paid_usd': total_fee_paid_usd,
        'average_fee_paid_usd': average_fee_paid_usd
    }