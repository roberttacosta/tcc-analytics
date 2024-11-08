import json
import os
from bson import ObjectId
import redis
import pickle
import logging

# Configuração do Redis
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)


def format_currency(value):
    return f"{int(value) / 1e18:.2f} ETH"


def calculate_gas_cost(gas, gas_price):
    return gas * gas_price


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        return super().default(obj)


def save_to_text(data, filename):
    os.makedirs("results", exist_ok=True)
    path = os.path.join("results", filename)

    with open(path, "w") as f:
        for item in data:
            f.write(json.dumps(item, cls=CustomJSONEncoder) + "\n")
    print(f"Dados salvos em {path}")


def get_from_cache(cache_key):
    cached_data = redis_client.get(cache_key)
    if cached_data:
        logging.info("Dados carregados do cache Redis.")
        return pickle.loads(cached_data)
    return None


def save_to_cache(cache_key, data):
    redis_client.set(cache_key, pickle.dumps(data))
    logging.info("Dados salvos no cache Redis.")
