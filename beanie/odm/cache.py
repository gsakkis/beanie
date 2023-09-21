from collections import OrderedDict
from datetime import datetime, timedelta
from typing import Awaitable, Callable, Generic, Tuple, TypeVar

K = TypeVar("K")
V = TypeVar("V")


class LRUCache(Generic[K, V]):
    def __init__(self, capacity: int, expiration_time: timedelta):
        self._capacity = capacity
        self._expiration_time = expiration_time
        self._cache: OrderedDict[K, Tuple[V, datetime]] = OrderedDict()

    async def get(self, key: K, get_value: Callable[[], Awaitable[V]]) -> V:
        cache = self._cache
        try:
            cached_entry = cache.pop(key)
            if datetime.utcnow() - cached_entry[1] <= self._expiration_time:
                cache[key] = cached_entry
                return cached_entry[0]
        except KeyError:
            pass

        value = await get_value()
        if len(cache) >= self._capacity:
            cache.popitem(last=False)
        cache[key] = (value, datetime.utcnow())
        return value
