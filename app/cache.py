from cachetools import TTLCache

cache_container = TTLCache(maxsize=100, ttl=600)

def get_cache_value(key:str):
    return cache_container.get(key)

def put_value_to_cache(key, value):
    cache_container[key] = value