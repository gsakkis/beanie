from abc import abstractmethod
from typing import Any, Dict, Optional, Type

from beanie.odm.cache import LRUCache
from beanie.odm.interfaces.settings import SettingsInterface


class Cacheable:
    _caches: Dict[type, LRUCache] = {}

    def __init__(
        self,
        document_model: Type[SettingsInterface],
        ignore_cache: bool = False,
    ):
        self.document_model = document_model
        self.ignore_cache = ignore_cache

    @property
    def _cache(self) -> Optional[LRUCache]:
        if self.ignore_cache:
            return None
        settings = self.document_model.get_settings()
        if not settings.use_cache:
            return None
        try:
            return self._caches[self.document_model]
        except KeyError:
            cache = LRUCache(
                capacity=settings.cache_capacity,
                expiration_time=settings.cache_expiration_time,
            )
            return self._caches.setdefault(self.document_model, cache)

    @property
    def _cache_key(self) -> str:
        return str(self._cache_key_dict())

    @abstractmethod
    def _cache_key_dict(self) -> Dict[str, Any]:
        ...
