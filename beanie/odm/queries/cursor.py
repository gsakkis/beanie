from abc import abstractmethod
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

from beanie.odm.interfaces.settings import SettingsInterface
from beanie.odm.queries.cacheable import Cacheable
from beanie.odm.utils.parsing import ParseableModel, parse_obj

ProjectionT = TypeVar("ProjectionT", bound=Union[BaseModel, Mapping[str, Any]])


class BaseCursorQuery(Cacheable, Generic[ProjectionT]):
    """
    BaseCursorQuery class. Wrapper over AsyncIOMotorCursor,
    which parse result with model
    """

    def __init__(
        self,
        document_model: Type[SettingsInterface],
        projection_model: Optional[Type[ParseableModel]] = None,
        ignore_cache: bool = False,
    ):
        super().__init__(document_model, ignore_cache)
        self._cursor: Optional[AgnosticBaseCursor] = None
        self.projection_model = projection_model
        self.lazy_parse = False

    def __aiter__(self):
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
        cache = self._cache
        items: Union[List[BaseModel], List[Mapping[str, Any]]]
        if cache is None:
            items = await self._motor_cursor.to_list(length)
        else:
            cache_key = self._cache_key
            items = cache.get(cache_key)
            if items is None:
                items = await self._motor_cursor.to_list(length)
            cache.set(cache_key, items)

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
