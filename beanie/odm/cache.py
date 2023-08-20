from collections import OrderedDict
from datetime import datetime, timedelta


class LRUCache:
    def __init__(self, capacity: int, expiration_time: timedelta):
        self.capacity: int = capacity
        self.expiration_time: timedelta = expiration_time
        self.cache: OrderedDict = OrderedDict()

    def get(self, key):
        try:
            item = self.cache.pop(key)
            if datetime.utcnow() - item[1] > self.expiration_time:
                return None
            self.cache[key] = item
            return item[0]
        except KeyError:
            return None

    def set(self, key, value):
        try:
            self.cache.pop(key)
        except KeyError:
            if len(self.cache) >= self.capacity:
                self.cache.popitem(last=False)
        self.cache[key] = (value, datetime.utcnow())

    @staticmethod
    def create_key(*args):
        return str(args)  # TODO think about this
