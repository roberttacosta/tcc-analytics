import pandas as pd
from pymongo import MongoClient
from bson import ObjectId
from src.utils.helpers import get_from_cache, save_to_cache
import json


def analyze_flash_loan_wallets(use_cache=True):
    cache_key_polygon = 'flash_loan_wallets_analysis_polygon'
    cache_key_ethereum = 'flash_loan_wallets_analysis_ethereum'

    if use_cache:
        cached_data_polygon = get_from_cache(cache_key_polygon)
        cached_data_ethereum = get_from_cache(cache_key_ethereum)
        if cached_data_polygon is not None and cached_data_ethereum is not None:
            return pd.DataFrame(json.loads(cached_data_polygon)), pd.DataFrame(json.loads(cached_data_ethereum))

    client = MongoClient()
    db = client['defi_data']
    collection = db['transactions']

    # Query to get wallets and their networks for flash loans
    flash_loan_wallets = collection.aggregate([
        {"$match": {"function_name": {"$in": ["flashLoan", "flashLoanSimple"]}}},
        {"$group": {"_id": {"wallet": "$from", "network": "$network"}}}
    ])

    # Flatten the aggregation result
    flash_loan_wallets_data = pd.DataFrame(list(flash_loan_wallets))
    flash_loan_wallets_data = pd.concat(
        [flash_loan_wallets_data['_id'].apply(pd.Series), flash_loan_wallets_data.drop(columns=['_id'])], axis=1)

    # Select 20 wallets from each network
    polygon_wallets = flash_loan_wallets_data[flash_loan_wallets_data['network'] == 'polygon'].head(20)[
        'wallet'].tolist()
    ethereum_wallets = flash_loan_wallets_data[flash_loan_wallets_data['network'] == 'ethereum'].head(20)[
        'wallet'].tolist()

    # Analyze the next 5 interactions of these wallets on the same network
    transactions_data_polygon = []
    transactions_data_ethereum = []

    for wallet in polygon_wallets:
        network = 'polygon'
        flash_loan_transactions = list(collection.find(
            {"from": wallet, "network": network, "function_name": {"$in": ["flashLoan", "flashLoanSimple"]}}).sort(
            "timestamp", 1))

        for flash_loan_transaction in flash_loan_transactions:
            flash_loan_transaction['_id'] = str(flash_loan_transaction['_id'])
            flash_loan_transaction['wallet'] = wallet  # Add wallet to each transaction
            transactions_data_polygon.append(flash_loan_transaction)

            # Get the next 5 transactions after the flash loan transaction
            next_transactions = list(collection.find(
                {"from": wallet, "network": network, "timestamp": {"$gt": flash_loan_transaction['timestamp']}}).sort(
                "timestamp", 1).limit(5))
            for transaction in next_transactions:
                transaction['_id'] = str(transaction['_id'])
                transaction['wallet'] = wallet  # Add wallet to each transaction
                transactions_data_polygon.append(transaction)

    for wallet in ethereum_wallets:
        network = 'ethereum'
        flash_loan_transactions = list(collection.find(
            {"from": wallet, "network": network, "function_name": {"$in": ["flashLoan", "flashLoanSimple"]}}).sort(
            "timestamp", 1))

        for flash_loan_transaction in flash_loan_transactions:
            flash_loan_transaction['_id'] = str(flash_loan_transaction['_id'])
            flash_loan_transaction['wallet'] = wallet  # Add wallet to each transaction
            transactions_data_ethereum.append(flash_loan_transaction)

            # Get the next 5 transactions after the flash loan transaction
            next_transactions = list(collection.find(
                {"from": wallet, "network": network, "timestamp": {"$gt": flash_loan_transaction['timestamp']}}).sort(
                "timestamp", 1).limit(5))
            for transaction in next_transactions:
                transaction['_id'] = str(transaction['_id'])
                transaction['wallet'] = wallet  # Add wallet to each transaction
                transactions_data_ethereum.append(transaction)

    transactions_df_polygon = pd.DataFrame(transactions_data_polygon)
    transactions_df_ethereum = pd.DataFrame(transactions_data_ethereum)

    save_to_cache(cache_key_polygon, json.dumps(transactions_df_polygon.to_dict(orient='records')))
    save_to_cache(cache_key_ethereum, json.dumps(transactions_df_ethereum.to_dict(orient='records')))

    return transactions_df_polygon, transactions_df_ethereum
