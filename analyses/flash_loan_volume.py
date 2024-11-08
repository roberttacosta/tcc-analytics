import pandas as pd
from pymongo import MongoClient
from src.utils.helpers import get_from_cache, save_to_cache
import json


def analyze_flash_loan_volume(use_cache=True, separate_by_network=False):
    cache_key = 'flash_loan_volume'

    if use_cache:
        cached_data = get_from_cache(cache_key)
        if cached_data is not None:
            return pd.DataFrame(json.loads(cached_data))

    client = MongoClient()
    db = client['defi_data']
    collection = db['transactions']

    function_names = collection.distinct("function_name")
    networks = collection.distinct("network")

    results = []

    for function_name in function_names:
        for network in networks:
            for is_error in [0, 1]:
                count = collection.count_documents({
                    'function_name': function_name,
                    'network': network,
                    'is_error': is_error
                })
                results.append({
                    'function_name': function_name,
                    'network': network,
                    'is_error': is_error,
                    'count': count
                })

    volume_data = pd.DataFrame(results)
    save_to_cache(cache_key, json.dumps(results))
    return volume_data


def analyze_flash_loan_volume_all(use_cache=True, separate_by_network=True):
    cache_key = 'flash_loan_volume'

    if use_cache:
        cached_data = get_from_cache(cache_key)
        if cached_data is not None:
            volume_data = pd.DataFrame(json.loads(cached_data))
            # Filtrar para flashLoan e flashLoanSimple
            flash_loan_data = volume_data[volume_data['function_name'].isin(['flashLoan', 'flashLoanSimple'])]
            if separate_by_network:
                all_data = volume_data.groupby('network')['count'].sum().reset_index()
                flash_loan_data = flash_loan_data.groupby('network')['count'].sum().reset_index()
            else:
                all_data = pd.DataFrame([{'network': 'all', 'count': volume_data['count'].sum()}])
                flash_loan_data = pd.DataFrame([{'network': 'all', 'count': flash_loan_data['count'].sum()}])
            flash_loan_data['type'] = 'flashLoan'
            all_data['type'] = 'all'
            combined_data = pd.concat([flash_loan_data, all_data])
            return combined_data

    # Se o cache não for usado ou os dados não estiverem disponíveis no cache, usar o método original
    return analyze_flash_loan_volume(use_cache=False)