from pymongo import MongoClient, ASCENDING
import pandas as pd
import logging
import random

logging.basicConfig(level=logging.INFO)


def get_db():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['defi_data']
    return db


def create_indexes():
    db = get_db()
    collection = db['transactions']

    # Cria os índices necessários para otimizar as consultas
    collection.create_index([('function_name', ASCENDING)])
    collection.create_index([('timestamp', ASCENDING)])
    collection.create_index([('network', ASCENDING)])
    collection.create_index([('is_error', ASCENDING), ('function_name', ASCENDING), ('network', ASCENDING)])

    print("Índices criados com sucesso.")


def load_all_transactions(function_name=None, min_value=None):
    db = get_db()
    collection = db['transactions']

    query = {}
    if function_name:
        if isinstance(function_name, list):
            query['function_name'] = {"$in": function_name}
        else:
            query['function_name'] = function_name
    if min_value:
        query['value'] = {"$gte": min_value}

    # Adiciona o filtro is_error: 0
    query['is_error'] = 0

    # Log the query being executed
    logging.info(f"Executando consulta com filtro: {query}")

    # Executa a consulta com base no filtro definido
    transactions = pd.DataFrame(list(collection.find(query)))
    logging.info(f"{len(transactions)} transações carregadas.")

    return transactions
