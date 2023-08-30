from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Type, Union

from motor.core import AgnosticBaseCursor
from pydantic import BaseModel

from beanie.odm.cache import LRUCache
from beanie.odm.utils.parsing import ParseableModel, parse_obj


class BaseCursorQuery(ABC):
    """
    BaseCursorQuery class. Wrapper over AsyncIOMotorCursor,
    which parse result with model
    """

    _cursor: Optional[AgnosticBaseCursor] = None

    projection_model: Optional[Type[ParseableModel]]
    lazy_parse: bool

    def __aiter__(self):
        if self._cursor is None:
            self._cursor = self._motor_cursor
        return self

    async def __anext__(self) -> Union[BaseModel, Dict[str, Any]]:
        if self._cursor is None:
            raise RuntimeError("cursor was not set")
        next_item = await self._cursor.__anext__()
        if self.projection_model is None:
            return next_item
        return parse_obj(
            self.projection_model, next_item, lazy_parse=self.lazy_parse
        )

    async def to_list(
        self, length: Optional[int] = None
    ) -> Union[List[BaseModel], List[Dict[str, Any]]]:
        """
        Get list of documents

        :param length: Optional[int] - length of the list
        :return: Union[List[BaseModel], List[Dict[str, Any]]]
        """
        cursor = self._motor_cursor
        if cursor is None:
            raise RuntimeError("self._motor_cursor was not set")

        cache = self._cache
        if cache is None:
            motor_list = await cursor.to_list(length)
        else:
            cache_key = self._cache_key
            motor_list = cache.get(cache_key)
            if motor_list is None:
                motor_list = await cursor.to_list(length)
            cache.set(cache_key, motor_list)

        if self.projection_model is not None:
            motor_list = [
                parse_obj(self.projection_model, i, lazy_parse=self.lazy_parse)
                for i in motor_list
            ]
        return motor_list

    @property
    @abstractmethod
    def _motor_cursor(self) -> AgnosticBaseCursor:
        ...

    @property
    @abstractmethod
    def _cache(self) -> Optional[LRUCache]:
        ...

    @property
    @abstractmethod
    def _cache_key(self) -> str:
        ...
