import pandas as pd


def convert_timestamp(transactions):
    for tx in transactions:
        tx['timestamp'] = pd.to_datetime(tx['timestamp'], unit='s')
    return transactions


def filter_transactions(transactions, min_value=None):
    if min_value:
        return [tx for tx in transactions if float(tx.get("value", 0)) >= min_value]
    return transactions
