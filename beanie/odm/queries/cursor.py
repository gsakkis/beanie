from abc import ABC, abstractmethod
from typing import (
    Any,
    Dict,
    Generic,
    List,
    Mapping,
    Optional,
    TypeVar,
    Union,
    cast,
)

from motor.core import AgnosticBaseCursor
from pydantic import BaseModel

from beanie.exceptions import NotSupported
from beanie.odm.links import LinkedModelMixin
from beanie.odm.queries.find.base import FindQuery
from beanie.odm.utils.parsing import parse_obj

ProjectionT = TypeVar("ProjectionT", bound=Union[BaseModel, Dict[str, Any]])


class BaseCursorQuery(FindQuery, ABC, Generic[ProjectionT]):
    """
    BaseCursorQuery class. Wrapper over AsyncIOMotorCursor,
    which parse result with model
    """

    _cursor: Optional[AgnosticBaseCursor] = None

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

    def _build_aggregation_pipeline(
        self, *extra_stages: Mapping[str, Any], project: bool = True
    ) -> List[Mapping[str, Any]]:
        pipeline: List[Mapping[str, Any]] = []

        if self.fetch_links:
            document_model = self.document_model
            if not issubclass(document_model, LinkedModelMixin):
                raise NotSupported(
                    f"{document_model} doesn't support link fetching"
                )
            for link_info in document_model.get_link_fields().values():
                pipeline.extend(link_info.iter_pipeline_stages())

        if filter_query := self.get_filter_query():
            text_query = filter_query.pop("$text", None)
            if text_query is not None:
                pipeline.insert(0, {"$match": {"$text": text_query}})
            if filter_query:
                pipeline.append({"$match": filter_query})

        if extra_stages:
            pipeline.extend(extra_stages)

        if project and (projection := self._get_projection()) is not None:
            pipeline.append({"$project": projection})

        return pipeline

    @property
    @abstractmethod
    def _motor_cursor(self) -> AgnosticBaseCursor:
        ...
