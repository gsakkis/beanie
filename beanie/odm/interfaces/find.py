from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import (
    Any,
    ClassVar,
    Dict,
    List,
    Mapping,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from motor.motor_asyncio import AsyncIOMotorCollection
from pydantic import BaseModel
from pymongo.client_session import ClientSession

import beanie
from beanie.odm.fields import LinkInfo, SortDirection
from beanie.odm.queries.find import AggregationQuery, FindMany, FindOne
from beanie.odm.settings.base import ItemSettings

ModelT = TypeVar("ModelT", bound=BaseModel)


class FindInterface(ABC):
    _inheritance_inited: ClassVar[bool]
    _class_id: ClassVar[Optional[str]]
    _children: ClassVar[Dict[str, Type]]

    @classmethod
    @abstractmethod
    def get_link_fields(cls) -> Optional[Dict[str, LinkInfo]]:
        pass

    @classmethod
    @abstractmethod
    def get_settings(cls) -> ItemSettings:
        pass

    @classmethod
    def get_motor_collection(cls) -> AsyncIOMotorCollection:
        return cls.get_settings().motor_collection

    @classmethod
    def get_collection_name(cls) -> Optional[str]:
        return cls.get_settings().name

    @classmethod
    def find_one(
        cls,
        *args: Union[Mapping[str, Any], bool],
        projection_model: Optional[Type[ModelT]] = None,
        session: Optional[ClientSession] = None,
        ignore_cache: bool = False,
        fetch_links: bool = False,
        with_children: bool = False,
        **pymongo_kwargs: Any,
    ) -> FindOne[ModelT]:
        """
        Find one document by criteria.
        Returns [FindOne](https://roman-right.github.io/beanie/api/queries/#findone) query object.
        When awaited this will either return a document or None if no document exists for the search criteria.

        :param args: *Mapping[str, Any] - search criteria
        :param projection_model: Optional[Type[BaseModel]] - projection model
        :param session: Optional[ClientSession] - pymongo session instance
        :param ignore_cache: bool
        :param **pymongo_kwargs: pymongo native parameters for find operation (if Document class contains links, this parameter must fit the respective parameter of the aggregate MongoDB function)
        :return: [FindOne](https://roman-right.github.io/beanie/api/queries/#findone) - find query instance
        """
        args = cls._add_class_id_filter(args, with_children)
        return FindOne[ModelT](document_model=cls).find(
            *args,
            projection_model=projection_model,
            session=session,
            ignore_cache=ignore_cache,
            fetch_links=fetch_links,
            **pymongo_kwargs,
        )

    @classmethod
    def find_many(
        cls,
        *args: Union[Mapping[str, Any], bool],
        projection_model: Optional[Type[ModelT]] = None,
        skip: Optional[int] = None,
        limit: Optional[int] = None,
        sort: Union[None, str, List[Tuple[str, SortDirection]]] = None,
        session: Optional[ClientSession] = None,
        ignore_cache: bool = False,
        fetch_links: bool = False,
        with_children: bool = False,
        lazy_parse: bool = False,
        **pymongo_kwargs: Any,
    ) -> Union[FindMany[ModelT], FindMany[Dict[str, Any]]]:
        """
        Find many documents by criteria.
        Returns [FindMany](https://roman-right.github.io/beanie/api/queries/#findmany) query object

        :param args: *Mapping[str, Any] - search criteria
        :param skip: Optional[int] - The number of documents to omit.
        :param limit: Optional[int] - The maximum number of results to return.
        :param sort: Union[None, str, List[Tuple[str, SortDirection]]] - A key or a list of (key, direction) pairs specifying the sort order for this query.
        :param projection_model: Optional[Type[BaseModel]] - projection model
        :param session: Optional[ClientSession] - pymongo session
        :param ignore_cache: bool
        :param lazy_parse: bool
        :param **pymongo_kwargs: pymongo native parameters for find operation (if Document class contains links, this parameter must fit the respective parameter of the aggregate MongoDB function)
        :return: [FindMany](https://roman-right.github.io/beanie/api/queries/#findmany) - query instance
        """
        args = cls._add_class_id_filter(args, with_children)
        return FindMany[Any](document_model=cls).find(
            *args,
            sort=sort,
            skip=skip,
            limit=limit,
            projection_model=projection_model,
            session=session,
            ignore_cache=ignore_cache,
            fetch_links=fetch_links,
            lazy_parse=lazy_parse,
            **pymongo_kwargs,
        )

    @classmethod
    def find(
        cls,
        *args: Union[Mapping[str, Any], bool],
        projection_model: Optional[Type[ModelT]] = None,
        skip: Optional[int] = None,
        limit: Optional[int] = None,
        sort: Union[None, str, List[Tuple[str, SortDirection]]] = None,
        session: Optional[ClientSession] = None,
        ignore_cache: bool = False,
        fetch_links: bool = False,
        with_children: bool = False,
        lazy_parse: bool = False,
        **pymongo_kwargs: Any,
    ) -> Union[FindMany[ModelT], FindMany[Dict[str, Any]]]:
        """
        The same as find_many
        """
        return cls.find_many(
            *args,
            skip=skip,
            limit=limit,
            sort=sort,
            projection_model=projection_model,
            session=session,
            ignore_cache=ignore_cache,
            fetch_links=fetch_links,
            with_children=with_children,
            lazy_parse=lazy_parse,
            **pymongo_kwargs,
        )

    @classmethod
    def find_all(
        cls,
        *,
        projection_model: Optional[Type[ModelT]] = None,
        skip: Optional[int] = None,
        limit: Optional[int] = None,
        sort: Union[None, str, List[Tuple[str, SortDirection]]] = None,
        session: Optional[ClientSession] = None,
        ignore_cache: bool = False,
        with_children: bool = False,
        lazy_parse: bool = False,
        **pymongo_kwargs: Any,
    ) -> Union[FindMany[ModelT], FindMany[Dict[str, Any]]]:
        """
        Get all the documents

        :param skip: Optional[int] - The number of documents to omit.
        :param limit: Optional[int] - The maximum number of results to return.
        :param sort: Union[None, str, List[Tuple[str, SortDirection]]] - A key or a list of (key, direction) pairs specifying the sort order for this query.
        :param projection_model: Optional[Type[BaseModel]] - projection model
        :param session: Optional[ClientSession] - pymongo session
        :param **pymongo_kwargs: pymongo native parameters for find operation (if Document class contains links, this parameter must fit the respective parameter of the aggregate MongoDB function)
        :return: [FindMany](https://roman-right.github.io/beanie/api/queries/#findmany) - query instance
        """
        kwargs = dict(
            skip=skip,
            limit=limit,
            sort=sort,
            session=session,
            ignore_cache=ignore_cache,
            with_children=with_children,
            lazy_parse=lazy_parse,
            **pymongo_kwargs,
        )
        if projection_model is None:
            return cls.find_many({}, **kwargs)
        return cls.find_many({}, projection_model=projection_model, **kwargs)

    @classmethod
    def all(
        cls,
        projection_model: Optional[Type[ModelT]] = None,
        skip: Optional[int] = None,
        limit: Optional[int] = None,
        sort: Union[None, str, List[Tuple[str, SortDirection]]] = None,
        session: Optional[ClientSession] = None,
        ignore_cache: bool = False,
        with_children: bool = False,
        lazy_parse: bool = False,
        **pymongo_kwargs: Any,
    ) -> Union[FindMany[ModelT], FindMany[Dict[str, Any]]]:
        """
        the same as find_all
        """
        return cls.find_all(
            skip=skip,
            limit=limit,
            sort=sort,
            projection_model=projection_model,
            session=session,
            ignore_cache=ignore_cache,
            with_children=with_children,
            lazy_parse=lazy_parse,
            **pymongo_kwargs,
        )

    @classmethod
    async def count(cls) -> int:
        """
        Number of documents in the collections
        The same as find_all().count()

        :return: int
        """
        return await cls.find_all().count()

    @classmethod
    def aggregate(
        cls,
        aggregation_pipeline: list,
        projection_model: Optional[Type[ModelT]] = None,
        session: Optional[ClientSession] = None,
        ignore_cache: bool = False,
        **pymongo_kwargs: Any,
    ) -> Union[AggregationQuery[ModelT], AggregationQuery[Dict[str, Any]]]:
        """
        Aggregate over collection.
        Returns [AggregationQuery](https://roman-right.github.io/beanie/api/queries/#aggregationquery) query object
        :param aggregation_pipeline: list - aggregation pipeline
        :param projection_model: Type[BaseModel]
        :param session: Optional[ClientSession]
        :param ignore_cache: bool
        :param **pymongo_kwargs: pymongo native parameters for aggregate operation
        :return: [AggregationQuery](https://roman-right.github.io/beanie/api/queries/#aggregationquery)
        """
        return cls.find_all(projection_model=projection_model).aggregate(
            aggregation_pipeline=aggregation_pipeline,
            projection_model=projection_model,
            session=session,
            ignore_cache=ignore_cache,
            **pymongo_kwargs,
        )

    @classmethod
    def _add_class_id_filter(cls, args: Tuple, with_children: bool = False):
        class_id = cls.get_settings().class_id
        # skip if _class_id is already added
        if any(isinstance(a, Iterable) and class_id in a for a in args):
            return args

        if issubclass(cls, beanie.Document) and cls._inheritance_inited:
            class_id_filter = (
                {"$in": [cls._class_id, *cls._children.keys()]}
                if with_children
                else cls._class_id
            )
            args += ({class_id: class_id_filter},)

        if cls.get_settings().union_doc:
            args += ({class_id: cls.get_settings().union_doc_alias},)
        return args
