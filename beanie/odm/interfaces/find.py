from datetime import timedelta
from typing import (
    Any,
    ClassVar,
    Dict,
    Generic,
    List,
    Mapping,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

from motor.motor_asyncio import AsyncIOMotorCollection, AsyncIOMotorDatabase
from pydantic import BaseModel, ConfigDict, Field
from pymongo.client_session import ClientSession

from beanie.odm.fields import SortDirection
from beanie.odm.operators import FieldName
from beanie.odm.queries.aggregation import AggregationQuery
from beanie.odm.queries.find import FindMany, FindOne


class BaseSettings(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str
    database: AsyncIOMotorDatabase
    class_id: str = "_class_id"

    use_cache: bool = False
    cache_capacity: int = 32
    cache_expiration_time: timedelta = timedelta(minutes=10)
    bson_encoders: Dict[Any, Any] = Field(default_factory=dict)

    @property
    def motor_collection(self) -> AsyncIOMotorCollection:
        return self.database[self.name]


SettingsT = TypeVar("SettingsT", bound=BaseSettings)
ModelT = TypeVar("ModelT", bound=BaseModel)


class FindInterface(Generic[SettingsT]):
    # The concrete type of `SettingsT` to be defined in subclasses. Unfortunately this
    # need to be duplicated both in the base class and as a ClassVar:
    # class Foo(FindInterface[FooSettings]):
    #     _settings_type = FooSettings
    _settings_type: ClassVar[Type[BaseSettings]]

    # Should be ClassVar[SettingsT] but ClassVars cannot contain type variable
    _settings: ClassVar[BaseSettings]

    @classmethod
    def set_settings(cls, database: AsyncIOMotorDatabase) -> None:
        settings = dict(name=cls.__name__, database=database)
        if hasattr(cls, "Settings"):
            settings.update(vars(cls.Settings))
        cls._settings = cls._settings_type.model_validate(settings)

    @classmethod
    def get_settings(cls) -> SettingsT:
        return cast(SettingsT, cls._settings)

    @classmethod
    def get_motor_collection(cls) -> AsyncIOMotorCollection:
        return cls.get_settings().motor_collection

    @classmethod
    def get_collection_name(cls) -> str:
        return cls.get_settings().name

    @classmethod
    def find_one(
        cls,
        *args: Mapping[FieldName, Any],
        projection_model: Optional[Type[ModelT]] = None,
        session: Optional[ClientSession] = None,
        ignore_cache: bool = False,
        fetch_links: bool = False,
        with_children: bool = False,
        **pymongo_kwargs: Any,
    ) -> FindOne[ModelT]:
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
        *args: Mapping[FieldName, Any],
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
    ) -> Union[FindMany[ModelT], FindMany[Dict[str, Any]]]:
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
        *args: Mapping[FieldName, Any],
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
        Returns [AggregationQuery](query.md#aggregationquery)
        :param aggregation_pipeline: list - aggregation pipeline
        :param projection_model: Type[BaseModel]
        :param session: Optional[ClientSession]
        :param ignore_cache: bool
        :keyword **pymongo_kwargs: pymongo native parameters for aggregate operation
        :return: [AggregationQuery](query.md#aggregationquery)
        """
        return cls.find_all(projection_model=projection_model).aggregate(
            aggregation_pipeline=aggregation_pipeline,
            projection_model=projection_model,
            session=session,
            ignore_cache=ignore_cache,
            **pymongo_kwargs,
        )

    @classmethod
    def _add_class_id_filter(
        cls, *args: Mapping, with_children: bool
    ) -> Tuple[Mapping, ...]:
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
