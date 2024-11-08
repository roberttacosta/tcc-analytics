import pandas as pd
from src.data.data_loader import load_all_transactions
from src.utils.helpers import get_from_cache, save_to_cache


def analyze_flash_loan_fee(use_cache=True):
    cache_key = 'flash_loan_fee'

    if use_cache:
        cached_data = get_from_cache(cache_key)
        if cached_data:
            return cached_data

    flash_loans = load_all_transactions(function_name='flashLoan')
    flash_loans['timestamp'] = pd.to_datetime(flash_loans['timestamp'])

    avg_fee = flash_loans['fee'].mean()
    fee_data = flash_loans.groupby(flash_loans['timestamp'].dt.date)['fee'].sum().reset_index(name='total_fee')

    save_to_cache(cache_key, (avg_fee, fee_data))
    return avg_fee, fee_data
