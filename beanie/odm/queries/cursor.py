from __future__ import annotations

from abc import ABC, abstractmethod
from typing import (
    Any,
    Dict,
    Generic,
    List,
    Optional,
    Type,
    TypeVar,
    Union,
    cast,
)

from motor.core import AgnosticBaseCursor
from pydantic import BaseModel

from beanie.odm.cache import LRUCache
from beanie.odm.utils.parsing import ParseableModel, parse_obj

ProjectionT = TypeVar("ProjectionT", bound=Union[BaseModel, Dict[str, Any]])


class BaseCursorQuery(ABC, Generic[ProjectionT]):
    """
    BaseCursorQuery class. Wrapper over AsyncIOMotorCursor,
    which parse result with model
    """

    _cursor: Optional[AgnosticBaseCursor] = None

    projection_model: Optional[Type[ParseableModel]] = None
    lazy_parse: bool = False

    def __aiter__(self):
        if self._cursor is None:
            self._cursor = self._motor_cursor
        return self

    async def __anext__(self) -> ProjectionT:
        if self._cursor is None:
            raise RuntimeError("cursor was not set")
        next_item = await self._cursor.__anext__()
        if self.projection_model is None:
            return next_item
        parsed_item = parse_obj(
            self.projection_model, next_item, lazy_parse=self.lazy_parse
        )
        return cast(ProjectionT, parsed_item)

    async def to_list(self, length: Optional[int] = None) -> List[ProjectionT]:
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

        projection_model = self.projection_model
        if projection_model is None:
            return motor_list
        return [
            cast(
                ProjectionT,
                parse_obj(projection_model, i, lazy_parse=self.lazy_parse),
            )
            for i in motor_list
        ]

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
