from abc import abstractmethod
from dataclasses import dataclass
from functools import partial
from typing import (
    Any,
    Generic,
    List,
    Mapping,
    Optional,
    Type,
    TypeVar,
    Union,
    cast,
)

from motor.core import AgnosticBaseCursor
from pydantic import BaseModel
from typing_extensions import Self

from beanie.odm.queries import CacheableQuery
from beanie.odm.utils.parsing import ParseableModel, parse_obj

ProjectionT = TypeVar("ProjectionT")


@dataclass
class BaseCursorQuery(CacheableQuery, Generic[ProjectionT]):
    """
    BaseCursorQuery class. Wrapper over AsyncIOMotorCursor,
    which parse result with model
    """

    projection_model: Optional[Type[ParseableModel]] = None
    lazy_parse: bool = False
    _cursor: Optional[AgnosticBaseCursor] = None

    def __aiter__(self) -> Self:
        if self._cursor is None:
            self._cursor = self._motor_cursor
        return self

    async def __anext__(self) -> ProjectionT:
        if self._cursor is None:
            raise RuntimeError("cursor was not set")

        next_item: Union[BaseModel, Mapping[str, Any]]
        next_item = await self._cursor.__anext__()
        if self.projection_model is not None:
            next_item = parse_obj(
                self.projection_model, next_item, lazy_parse=self.lazy_parse
            )
        return cast(ProjectionT, next_item)

    async def to_list(self, length: Optional[int] = None) -> List[ProjectionT]:
        """
        Get list of documents

        :param length: Optional[int] - length of the list
        :return: Union[List[BaseModel], List[Dict[str, Any]]]
        """
        items: Union[List[BaseModel], List[Mapping[str, Any]]]
        cache = self.document_model.get_cache()
        if cache is None or self.ignore_cache:
            items = await self._motor_cursor.to_list(length)
        else:
            items = await cache.get(
                self._cache_key, partial(self._motor_cursor.to_list, length)
            )
        projection_model = self.projection_model
        if projection_model is not None:
            items = [
                parse_obj(projection_model, i, lazy_parse=self.lazy_parse)
                for i in items
            ]
        return cast(List[ProjectionT], items)

    async def first_or_none(self) -> Optional[ProjectionT]:
        """
        Returns the first found element or None if no elements were found
        """
        res = await self.to_list(length=1)
        return res[0] if res else None

    @property
    @abstractmethod
    def _motor_cursor(self) -> AgnosticBaseCursor:
        ...
