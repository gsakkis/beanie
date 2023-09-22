from typing import (
    Any,
    List,
    Mapping,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    overload,
)

from pydantic import BaseModel
from pymongo.client_session import ClientSession
from typing_extensions import Self

import beanie
from beanie.odm.fields import SortDirection
from beanie.odm.interfaces.settings import SettingsInterface, SettingsT
from beanie.odm.operators import FieldName, FieldNameMapping
from beanie.odm.queries.aggregation import AggregationQuery
from beanie.odm.queries.find import FindMany, FindOne
from beanie.odm.utils.parsing import ParseableModel

ModelT = TypeVar("ModelT", bound=BaseModel)


class FindInterface(SettingsInterface[SettingsT]):
    @overload
    @classmethod
    def find_one(
        cls,
        *args: FieldNameMapping,
        projection_model: None = None,
        session: Optional[ClientSession] = None,
        ignore_cache: bool = False,
        fetch_links: bool = False,
        with_children: bool = False,
        **pymongo_kwargs: Any,
    ) -> FindOne[Self]:  # type: ignore[type-var]
        ...

    @overload
    @classmethod
    def find_one(
        cls,
        *args: FieldNameMapping,
        projection_model: Type[ModelT],
        session: Optional[ClientSession] = None,
        ignore_cache: bool = False,
        fetch_links: bool = False,
        with_children: bool = False,
        **pymongo_kwargs: Any,
    ) -> FindOne[ModelT]:
        ...

    @classmethod
    def find_one(
        cls,
        *args: FieldNameMapping,
        projection_model: Optional[Type[ModelT]] = None,
        session: Optional[ClientSession] = None,
        ignore_cache: bool = False,
        fetch_links: bool = False,
        with_children: bool = False,
        **pymongo_kwargs: Any,
    ) -> FindOne[Any]:
        """
        Find one document by criteria.
        Returns [FindOne](query.md#findone) query object.
        When awaited this will either return a document or None if no document exists for the search criteria.

        :param args: *Mapping[str, Any] - search criteria
        :param projection_model: Optional[Type[BaseModel]] - projection model
        :param session: Optional[ClientSession] - pymongo session instance
        :param ignore_cache: bool
        :keyword **pymongo_kwargs: pymongo native parameters for find operation.
            If Document class contains links, this parameter must fit the respective
            parameter of the aggregate MongoDB function.
        :return: [FindOne](query.md#findone) - find query instance
        """
        args = cls._add_class_id_filter(*args, with_children=with_children)
        return FindOne(document_model=cls).find(
            *args,
            projection_model=cls._get_parseable_model(projection_model),
            session=session,
            ignore_cache=ignore_cache,
            fetch_links=fetch_links,
            **pymongo_kwargs,
        )

    @overload
    @classmethod
    def find_many(
        cls,
        *args: FieldNameMapping,
        projection_model: None = None,
        skip: Optional[int] = None,
        limit: Optional[int] = None,
        sort: Union[
            None, FieldName, List[Tuple[FieldName, SortDirection]]
        ] = None,
        session: Optional[ClientSession] = None,
        ignore_cache: bool = False,
        fetch_links: bool = False,
        with_children: bool = False,
        lazy_parse: bool = False,
        **pymongo_kwargs: Any,
    ) -> FindMany[Self]:  # type: ignore[type-var]
        ...

    @overload
    @classmethod
    def find_many(
        cls,
        *args: FieldNameMapping,
        projection_model: Type[ModelT],
        skip: Optional[int] = None,
        limit: Optional[int] = None,
        sort: Union[
            None, FieldName, List[Tuple[FieldName, SortDirection]]
        ] = None,
        session: Optional[ClientSession] = None,
        ignore_cache: bool = False,
        fetch_links: bool = False,
        with_children: bool = False,
        lazy_parse: bool = False,
        **pymongo_kwargs: Any,
    ) -> FindMany[ModelT]:
        ...

    @classmethod
    def find_many(
        cls,
        *args: FieldNameMapping,
        projection_model: Optional[Type[ModelT]] = None,
        skip: Optional[int] = None,
        limit: Optional[int] = None,
        sort: Union[
            None, FieldName, List[Tuple[FieldName, SortDirection]]
        ] = None,
        session: Optional[ClientSession] = None,
        ignore_cache: bool = False,
        fetch_links: bool = False,
        with_children: bool = False,
        lazy_parse: bool = False,
        **pymongo_kwargs: Any,
    ) -> FindMany[Any]:
        """
        Find many documents by criteria.
        Returns [FindMany](query.md#findmany) query object

        :param args: *Mapping[str, Any] - search criteria
        :param skip: Optional[int] - The number of documents to omit.
        :param limit: Optional[int] - The maximum number of results to return.
        :param sort: Union[None, str, List[Tuple[str, SortDirection]]] - A key or a list of (key, direction) pairs specifying the sort order for this query.
        :param projection_model: Optional[Type[BaseModel]] - projection model
        :param session: Optional[ClientSession] - pymongo session
        :param ignore_cache: bool
        :param lazy_parse: bool
        :keyword **pymongo_kwargs: pymongo native parameters for find operation.
            If Document class contains links, this parameter must fit the respective
            parameter of the aggregate MongoDB function.
        :return: [FindMany](query.md#findmany) - query instance
        """
        args = cls._add_class_id_filter(*args, with_children=with_children)
        return FindMany(document_model=cls).find(
            *args,
            sort=sort,
            skip=skip,
            limit=limit,
            projection_model=cls._get_parseable_model(projection_model),
            session=session,
            ignore_cache=ignore_cache,
            fetch_links=fetch_links,
            lazy_parse=lazy_parse,
            **pymongo_kwargs,
        )

    # alias for find_many
    find = find_many

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
    ) -> FindMany[Any]:
        """
        Get all the documents

        :param skip: Optional[int] - The number of documents to omit.
        :param limit: Optional[int] - The maximum number of results to return.
        :param sort: Union[None, str, List[Tuple[str, SortDirection]]] - A key or a list of (key, direction) pairs specifying the sort order for this query.
        :param projection_model: Optional[Type[BaseModel]] - projection model
        :param session: Optional[ClientSession] - pymongo session
        :keyword **pymongo_kwargs: pymongo native parameters for find operation.
            If Document class contains links, this parameter must fit the respective
            parameter of the aggregate MongoDB function.
        :return: [FindMany](query.md#findmany) - query instance
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
            return cls.find_many(**kwargs)
        return cls.find_many(projection_model=projection_model, **kwargs)

    # alias for find_all
    all = find_all

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
        aggregation_pipeline: List[Mapping[str, Any]],
        projection_model: Optional[Type[ModelT]] = None,
        session: Optional[ClientSession] = None,
        ignore_cache: bool = False,
        **pymongo_kwargs: Any,
    ) -> Union[AggregationQuery[ModelT], AggregationQuery[Mapping[str, Any]]]:
        """
        Aggregate over collection.
        Returns [AggregationQuery](query.md#aggregationquery)
        :param aggregation_pipeline: list - aggregation pipeline
        :param projection_model: Type[BaseModel]
        :param session: Optional[ClientSession]
        :param ignore_cache: bool
        :keyword **pymongo_kwargs: pymongo native parameters for aggregate operation
        :return: [AggregationQuery](query.md#aggregationquery)
        """
        return cls.find_all().aggregate(
            aggregation_pipeline=aggregation_pipeline,
            projection_model=projection_model,
            session=session,
            ignore_cache=ignore_cache,
            **pymongo_kwargs,
        )

    @classmethod
    def _get_parseable_model(cls, t: Optional[type]) -> Type[ParseableModel]:
        parseable_model = t or cls
        if not (
            issubclass(parseable_model, BaseModel)
            or issubclass(parseable_model, beanie.UnionDoc)
        ):
            raise TypeError("projection_model must be BaseModel or UnionDoc")
        return parseable_model

    @classmethod
    def _add_class_id_filter(
        cls, *args: FieldNameMapping, with_children: bool
    ) -> Tuple[FieldNameMapping, ...]:
        class_id = cls.get_settings().class_id
        # skip if _class_id is already added
        if any(class_id in a for a in args):
            return args

        class_id_filter = cls._get_class_id_filter(with_children)
        if class_id_filter is None:
            return args

        return *args, {class_id: class_id_filter}

    @classmethod
    def _get_class_id_filter(
        cls, with_children: bool = False
    ) -> Optional[Any]:
        return None
