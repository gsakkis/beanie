from abc import abstractmethod
from typing import (
    TYPE_CHECKING,
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

from beanie.odm.queries.cacheable import Cacheable
from beanie.odm.utils.parsing import ParseableModel, parse_obj

ProjectionT = TypeVar("ProjectionT", bound=Union[BaseModel, Dict[str, Any]])

if TYPE_CHECKING:
    from beanie.odm.interfaces.find import FindInterface


class BaseCursorQuery(Cacheable, Generic[ProjectionT]):
    """
    BaseCursorQuery class. Wrapper over AsyncIOMotorCursor,
    which parse result with model
    """

    def __init__(
        self,
        document_model: Type["FindInterface"],
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
        cache = self._cache
        if cache is None:
            motor_list = await self._motor_cursor.to_list(length)
        else:
            cache_key = self._cache_key
            motor_list = cache.get(cache_key)
            if motor_list is None:
                motor_list = await self._motor_cursor.to_list(length)
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
