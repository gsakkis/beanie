from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generator,
    List,
    Mapping,
    Optional,
    Tuple,
    Type,
    Union,
    cast,
)

from motor.core import AgnosticBaseCursor
from pydantic import BaseModel
from pymongo import ReplaceOne
from pymongo.client_session import ClientSession
from pymongo.results import UpdateResult
from typing_extensions import Self

import beanie
from beanie.exceptions import DocumentNotFound
from beanie.odm.bulk import BulkWriter, Operation
from beanie.odm.cache import LRUCache
from beanie.odm.fields import SortDirection
from beanie.odm.interfaces.aggregation_methods import AggregateMethods
from beanie.odm.interfaces.session import SessionMethods
from beanie.odm.interfaces.update import UpdateMethods
from beanie.odm.operators.find.logical import And
from beanie.odm.queries.cursor import BaseCursorQuery
from beanie.odm.queries.delete import DeleteMany, DeleteOne
from beanie.odm.queries.update import UpdateMany, UpdateOne, UpdateResponse
from beanie.odm.utils.dump import get_dict
from beanie.odm.utils.encoder import Encoder
from beanie.odm.utils.find import construct_lookup_queries
from beanie.odm.utils.parsing import ParseableModel, parse_obj
from beanie.odm.utils.projection import get_projection
from beanie.odm.utils.relations import convert_ids

if TYPE_CHECKING:
    from beanie.odm.interfaces.find import FindInterface


class FindQuery(SessionMethods):
    """
    Find Query base class

    Inherited from:

    - [SessionMethods](https://roman-right.github.io/beanie/api/interfaces/#sessionmethods)
    """

    def __init__(
        self,
        document_model: Type["FindInterface"],
        projection_model: Optional[Type[ParseableModel]],
        ignore_cache: bool = False,
        **pymongo_kwargs: Any,
    ):
        self.document_model = document_model
        self.projection_model = projection_model
        self.find_expressions: List[Dict[str, Any]] = []
        self.pymongo_kwargs = pymongo_kwargs
        self.ignore_cache = ignore_cache
        self.fetch_links = False
        self.lazy_parse = False
        self.session = None

    @property
    def cache(self) -> Optional[LRUCache]:
        if (
            not self.ignore_cache
            and self.document_model.get_settings().use_cache
            and issubclass(self.document_model, beanie.Document)
        ):
            return self.document_model._cache
        return None

    @property
    def cache_key(self) -> str:
        return str(self._cache_key_dict())

    def _cache_key_dict(self) -> Dict[str, Any]:
        return dict(
            type=self.__class__.__name__,
            filter=self.get_filter_query(),
            projection=self.get_projection(),
            fetch_links=self.fetch_links,
        )

    def get_filter_query(self) -> Mapping[str, Any]:
        """Returns: MongoDB filter query"""
        if self.document_model.get_link_fields() is not None:
            for i, query in enumerate(self.find_expressions):
                self.find_expressions[i] = convert_ids(
                    query, self.document_model, self.fetch_links
                )
        if self.find_expressions:
            return Encoder(
                custom_encoders=self.document_model.get_bson_encoders()
            ).encode(And(*self.find_expressions).query)
        return {}

    def get_projection(self) -> Optional[Dict[str, Any]]:
        if self.projection_model is not None:
            return get_projection(self.projection_model)
        return None

    def project(
        self, projection_model: Optional[Type[ParseableModel]] = None
    ) -> Self:
        """Apply projection parameter"""
        if projection_model is not None:
            self.projection_model = projection_model
        return self

    async def count(self) -> int:
        """
        Number of found documents
        :return: int
        """
        collection = self.document_model.get_motor_collection()
        return await collection.count_documents(self.get_filter_query())

    async def exists(self) -> bool:
        """
        If find query will return anything

        :return: bool
        """
        return await self.count() > 0


class AggregationQuery(FindQuery, BaseCursorQuery):
    """
    Aggregation Query

    Inherited from:

    - [FindQuery](https://roman-right.github.io/beanie/api/queries/#findquery)
    - [BaseCursorQuery](https://roman-right.github.io/beanie/api/queries/#basecursorquery) - async generator
    """

    def __init__(
        self,
        aggregation_pipeline: List[Mapping[str, Any]],
        find_query: Mapping[str, Any],
        document_model: Type["FindInterface"],
        projection_model: Optional[Type[ParseableModel]] = None,
        ignore_cache: bool = False,
        **pymongo_kwargs,
    ):
        self.aggregation_pipeline = aggregation_pipeline
        self.find_query = find_query
        super().__init__(
            document_model, projection_model, ignore_cache, **pymongo_kwargs
        )

    def _cache_key_dict(self) -> Dict[str, Any]:
        d = super()._cache_key_dict()
        d.update(filter=self.find_query, pipeline=self.aggregation_pipeline)
        return d

    @property
    def motor_cursor(self) -> AgnosticBaseCursor:
        return self.document_model.get_motor_collection().aggregate(
            self.get_aggregation_pipeline(),
            session=self.session,
            **self.pymongo_kwargs,
        )

    def get_aggregation_pipeline(self) -> List[Mapping[str, Any]]:
        pipeline: List[Mapping[str, Any]] = []
        if self.find_query:
            pipeline.append({"$match": self.find_query})
        pipeline.extend(self.aggregation_pipeline)
        if (projection := self.get_projection()) is not None:
            pipeline.append({"$project": projection})
        return pipeline


class FindMany(FindQuery, BaseCursorQuery, UpdateMethods, AggregateMethods):
    """
    Find Many query class

    Inherited from:

    - [FindQuery](https://roman-right.github.io/beanie/api/queries/#findquery)
    - [BaseCursorQuery](https://roman-right.github.io/beanie/api/queries/#basecursorquery) - async generator
    - [AggregateMethods](https://roman-right.github.io/beanie/api/interfaces/#aggregatemethods)

    """

    def __init__(self, document_model: Type["FindInterface"]):
        super().__init__(
            document_model, cast(Type[ParseableModel], document_model)
        )
        self.sort_expressions: List[Tuple[str, SortDirection]] = []
        self.skip_number = 0
        self.limit_number = 0

    def _cache_key_dict(self) -> Dict[str, Any]:
        d = super()._cache_key_dict()
        d.update(
            sort=self.sort_expressions,
            skip=self.skip_number,
            limit=self.limit_number,
        )
        return d

    @property
    def motor_cursor(self) -> AgnosticBaseCursor:
        if self.fetch_links:
            return self.document_model.get_motor_collection().aggregate(
                self.build_aggregation_pipeline(project=True),
                session=self.session,
                **self.pymongo_kwargs,
            )

        return self.document_model.get_motor_collection().find(
            filter=self.get_filter_query(),
            sort=self.sort_expressions,
            projection=self.get_projection(),
            skip=self.skip_number,
            limit=self.limit_number,
            session=self.session,
            **self.pymongo_kwargs,
        )

    def find_many(
        self,
        *args: Union[Mapping[str, Any], bool],
        projection_model: Optional[Type[ParseableModel]] = None,
        skip: Optional[int] = None,
        limit: Optional[int] = None,
        sort: Union[None, str, List[Tuple[str, SortDirection]]] = None,
        session: Optional[ClientSession] = None,
        ignore_cache: bool = False,
        fetch_links: bool = False,
        lazy_parse: bool = False,
        **pymongo_kwargs,
    ) -> Self:
        """
        Find many documents by criteria

        :param args: *Mapping[str, Any] - search criteria
        :param skip: Optional[int] - The number of documents to omit.
        :param limit: Optional[int] - The maximum number of results to return.
        :param sort: Union[None, str, List[Tuple[str, SortDirection]]] - A key
        or a list of (key, direction) pairs specifying the sort order
        for this query.
        :param projection_model: Optional[Type[BaseModel]] - projection model
        :param session: Optional[ClientSession] - pymongo session
        :param ignore_cache: bool
        :param **pymongo_kwargs: pymongo native parameters for find operation (if Document class contains links, this parameter must fit the respective parameter of the aggregate MongoDB function)
        :return: FindMany - query instance
        """
        self.find_expressions += args  # type: ignore # bool workaround
        self.skip(skip)
        self.limit(limit)
        self.sort(sort)
        self.project(projection_model)
        self.set_session(session=session)
        self.ignore_cache = ignore_cache
        self.fetch_links = fetch_links
        self.pymongo_kwargs.update(pymongo_kwargs)
        if lazy_parse is True:
            self.lazy_parse = lazy_parse
        return self

    # TODO probably merge FindOne and FindMany to one class to avoid this
    #  code duplication

    def find(
        self,
        *args: Union[Mapping[str, Any], bool],
        projection_model: Optional[Type[ParseableModel]] = None,
        skip: Optional[int] = None,
        limit: Optional[int] = None,
        sort: Union[None, str, List[Tuple[str, SortDirection]]] = None,
        session: Optional[ClientSession] = None,
        ignore_cache: bool = False,
        fetch_links: bool = False,
        lazy_parse: bool = False,
        **pymongo_kwargs,
    ) -> Self:
        """
        The same as `find_many(...)`
        """
        return self.find_many(
            *args,
            skip=skip,
            limit=limit,
            sort=sort,
            projection_model=projection_model,
            session=session,
            ignore_cache=ignore_cache,
            fetch_links=fetch_links,
            lazy_parse=lazy_parse,
            **pymongo_kwargs,
        )

    def sort(
        self,
        *args: Union[
            None,
            str,
            Tuple[str, SortDirection],
            List[Tuple[str, SortDirection]],
        ],
    ) -> Self:
        """
        Add sort parameters
        :param args: Union[str, Tuple[str, SortDirection],
        List[Tuple[str, SortDirection]]] - A key or a tuple (key, direction)
        or a list of (key, direction) pairs specifying
        the sort order for this query.
        :return: self
        """
        for arg in args:
            if arg is None:
                pass
            elif isinstance(arg, list):
                self.sort(*arg)
            elif isinstance(arg, tuple):
                self.sort_expressions.append(arg)
            elif isinstance(arg, str):
                if arg.startswith("+"):
                    self.sort_expressions.append(
                        (arg[1:], SortDirection.ASCENDING)
                    )
                elif arg.startswith("-"):
                    self.sort_expressions.append(
                        (arg[1:], SortDirection.DESCENDING)
                    )
                else:
                    self.sort_expressions.append(
                        (arg, SortDirection.ASCENDING)
                    )
            else:
                raise TypeError("Wrong argument type")
        return self

    def skip(self, n: Optional[int]) -> Self:
        """
        Set skip parameter
        :param n: int
        :return: self
        """
        if n is not None:
            self.skip_number = n
        return self

    def limit(self, n: Optional[int]) -> Self:
        """
        Set limit parameter
        :param n: int
        :return:
        """
        if n is not None:
            self.limit_number = n
        return self

    def update(
        self,
        *args: Mapping[str, Any],
        session: Optional[ClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        **pymongo_kwargs,
    ) -> UpdateMany:
        """
        Create Update with modifications query
        and provide search criteria there

        :param args: *Mapping[str,Any] - the modifications to apply.
        :param session: Optional[ClientSession]
        :param bulk_writer: Optional[BulkWriter]
        :return: UpdateMany query
        """
        if not issubclass(self.document_model, beanie.Document):
            raise RuntimeError("can update only beanie.Document")
        self.set_session(session)
        return (
            UpdateMany(
                document_model=self.document_model,
                find_query=self.get_filter_query(),
            )
            .update(*args, bulk_writer=bulk_writer, **pymongo_kwargs)
            .set_session(session=self.session)
        )

    def upsert(
        self,
        *args: Mapping[str, Any],
        on_insert: "beanie.Document",
        session: Optional[ClientSession] = None,
        **pymongo_kwargs,
    ) -> UpdateMany:
        """
        Create Update with modifications query
        and provide search criteria there

        :param args: *Mapping[str,Any] - the modifications to apply.
        :param on_insert: DocType - document to insert if there is no matched
        document in the collection
        :param session: Optional[ClientSession]
        :return: UpdateMany query
        """
        if not issubclass(self.document_model, beanie.Document):
            raise RuntimeError("can upsert only beanie.Document")
        self.set_session(session)
        return (
            UpdateMany(
                document_model=self.document_model,
                find_query=self.get_filter_query(),
            )
            .upsert(*args, on_insert=on_insert, **pymongo_kwargs)
            .set_session(session=self.session)
        )

    def update_many(
        self,
        *args: Mapping[str, Any],
        session: Optional[ClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        **pymongo_kwargs,
    ) -> UpdateMany:
        """
        Provide search criteria to the
        [UpdateMany](https://roman-right.github.io/beanie/api/queries/#updatemany) query

        :param args: *Mapping[str,Any] - the modifications to apply.
        :param session: Optional[ClientSession]
        :return: [UpdateMany](https://roman-right.github.io/beanie/api/queries/#updatemany) query
        """
        return self.update(
            *args, session=session, bulk_writer=bulk_writer, **pymongo_kwargs
        )

    def delete(
        self,
        session: Optional[ClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        **pymongo_kwargs,
    ) -> DeleteMany:
        """
        Provide search criteria to the Delete query

        :param session: Optional[ClientSession]
        :return: Union[DeleteOne, DeleteMany]
        """
        self.set_session(session=session)
        return DeleteMany(
            document_model=self.document_model,
            find_query=self.get_filter_query(),
            bulk_writer=bulk_writer,
            **pymongo_kwargs,
        ).set_session(session=session)

    def delete_many(
        self,
        session: Optional[ClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        **pymongo_kwargs,
    ) -> DeleteMany:
        """
        Provide search criteria to the [DeleteMany](https://roman-right.github.io/beanie/api/queries/#deletemany) query

        :param session:
        :return: [DeleteMany](https://roman-right.github.io/beanie/api/queries/#deletemany) query
        """
        return self.delete(
            session=session, bulk_writer=bulk_writer, **pymongo_kwargs
        )

    def aggregate(
        self,
        aggregation_pipeline: List[Any],
        projection_model: Optional[Type[ParseableModel]] = None,
        session: Optional[ClientSession] = None,
        ignore_cache: bool = False,
        **pymongo_kwargs,
    ) -> AggregationQuery:
        """
        Provide search criteria to the [AggregationQuery](https://roman-right.github.io/beanie/api/queries/#aggregationquery)

        :param aggregation_pipeline: list - aggregation pipeline. MongoDB doc:
        <https://docs.mongodb.com/manual/core/aggregation-pipeline/>
        :param projection_model: Type[BaseModel] - Projection Model
        :param session: Optional[ClientSession] - PyMongo session
        :param ignore_cache: bool
        :return:[AggregationQuery](https://roman-right.github.io/beanie/api/queries/#aggregationquery)
        """
        self.set_session(session=session)
        if not self.fetch_links:
            find_query = self.get_filter_query()
        else:
            aggregation_pipeline = self.build_aggregation_pipeline(
                *aggregation_pipeline
            )
            find_query = {}
        return AggregationQuery(
            aggregation_pipeline=aggregation_pipeline,
            document_model=self.document_model,
            projection_model=projection_model,
            find_query=find_query,
            ignore_cache=ignore_cache,
            **pymongo_kwargs,
        ).set_session(session=self.session)

    def build_aggregation_pipeline(
        self,
        *extra_stages: Dict[str, Any],
        project: bool = False,
    ) -> List[Dict[str, Any]]:
        aggregation_pipeline = construct_lookup_queries(self.document_model)
        aggregation_pipeline.append({"$match": self.get_filter_query()})
        if self.sort_expressions:
            aggregation_pipeline.append({"$sort": dict(self.sort_expressions)})
        if self.skip_number != 0:
            aggregation_pipeline.append({"$skip": self.skip_number})
        if self.limit_number != 0:
            aggregation_pipeline.append({"$limit": self.limit_number})
        aggregation_pipeline.extend(extra_stages)
        if project and (projection := self.get_projection()) is not None:
            aggregation_pipeline.append({"$project": projection})
        return aggregation_pipeline

    async def first_or_none(self) -> Union[BaseModel, Dict[str, Any], None]:
        """
        Returns the first found element or None if no elements were found
        """
        res = await self.limit(1).to_list()
        return res[0] if res else None

    async def count(self) -> int:
        """
        Number of found documents
        :return: int
        """
        if self.fetch_links:
            result = (
                await self.document_model.get_motor_collection()
                .aggregate(
                    self.build_aggregation_pipeline({"$count": "count"}),
                    session=self.session,
                    **self.pymongo_kwargs,
                )
                .to_list(length=1)
            )
            return result[0]["count"] if result else 0

        return await super().count()


class FindOne(FindQuery, UpdateMethods):
    """
    Find One query class

    Inherited from:

    - [FindQuery](https://roman-right.github.io/beanie/api/queries/#findquery)
    """

    # TODO probably merge FindOne and FindMany to one class to avoid this
    #  code duplication

    projection_model: Type[ParseableModel]

    def __init__(self, document_model: Type["FindInterface"]):
        super().__init__(
            document_model, cast(Type[ParseableModel], document_model)
        )

    def find_one(
        self,
        *args: Union[Mapping[str, Any], bool],
        projection_model: Optional[Type[ParseableModel]] = None,
        session: Optional[ClientSession] = None,
        ignore_cache: bool = False,
        fetch_links: bool = False,
        **pymongo_kwargs,
    ) -> Self:
        """
        Find one document by criteria

        :param args: *Mapping[str, Any] - search criteria
        :param projection_model: Optional[Type[BaseModel]] - projection model
        :param session: Optional[ClientSession] - pymongo session
        :param ignore_cache: bool
        :param **pymongo_kwargs: pymongo native parameters for find operation (if Document class contains links, this parameter must fit the respective parameter of the aggregate MongoDB function)
        :return: FindOne - query instance
        """
        self.find_expressions += args  # type: ignore # bool workaround
        self.project(projection_model)
        self.set_session(session=session)
        self.ignore_cache = ignore_cache
        self.fetch_links = fetch_links
        self.pymongo_kwargs.update(pymongo_kwargs)
        return self

    def update(
        self,
        *args: Mapping[str, Any],
        session: Optional[ClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        response_type: Optional[UpdateResponse] = None,
        **pymongo_kwargs,
    ) -> UpdateOne:
        """
        Create Update with modifications query
        and provide search criteria there

        :param args: *Mapping[str,Any] - the modifications to apply.
        :param session: Optional[ClientSession]
        :param bulk_writer: Optional[BulkWriter]
        :param response_type: Optional[UpdateResponse]
        :return: UpdateOne query
        """
        if not issubclass(self.document_model, beanie.Document):
            raise RuntimeError("can update only beanie.Document")
        self.set_session(session)
        return (
            UpdateOne(
                document_model=self.document_model,
                find_query=self.get_filter_query(),
            )
            .update(
                *args,
                bulk_writer=bulk_writer,
                response_type=response_type,
                **pymongo_kwargs,
            )
            .set_session(session=self.session)
        )

    def upsert(
        self,
        *args: Mapping[str, Any],
        on_insert: "beanie.Document",
        session: Optional[ClientSession] = None,
        response_type: Optional[UpdateResponse] = None,
        **pymongo_kwargs,
    ) -> UpdateOne:
        """
        Create Update with modifications query
        and provide search criteria there

        :param args: *Mapping[str,Any] - the modifications to apply.
        :param on_insert: DocType - document to insert if there is no matched
        document in the collection
        :param session: Optional[ClientSession]
        :param response_type: Optional[UpdateResponse]
        :return: UpdateOne query
        """
        if not issubclass(self.document_model, beanie.Document):
            raise RuntimeError("can upsert only beanie.Document")
        self.set_session(session)
        return (
            UpdateOne(
                document_model=self.document_model,
                find_query=self.get_filter_query(),
            )
            .upsert(
                *args,
                on_insert=on_insert,
                response_type=response_type,
                **pymongo_kwargs,
            )
            .set_session(session=self.session)
        )

    def update_one(
        self,
        *args: Mapping[str, Any],
        session: Optional[ClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        response_type: Optional[UpdateResponse] = None,
        **pymongo_kwargs,
    ) -> UpdateOne:
        """
        Create [UpdateOne](https://roman-right.github.io/beanie/api/queries/#updateone) query using modifications and
        provide search criteria there
        :param args: *Mapping[str,Any] - the modifications to apply
        :param session: Optional[ClientSession] - PyMongo sessions
        :param response_type: Optional[UpdateResponse]
        :return: [UpdateOne](https://roman-right.github.io/beanie/api/queries/#updateone) query
        """
        return self.update(
            *args,
            session=session,
            bulk_writer=bulk_writer,
            response_type=response_type,
            **pymongo_kwargs,
        )

    def delete(
        self,
        session: Optional[ClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        **pymongo_kwargs,
    ) -> DeleteOne:
        """
        Provide search criteria to the Delete query

        :param session: Optional[ClientSession]
        :return: Union[DeleteOne, DeleteMany]
        """
        self.set_session(session=session)
        return DeleteOne(
            document_model=self.document_model,
            find_query=self.get_filter_query(),
            bulk_writer=bulk_writer,
            **pymongo_kwargs,
        ).set_session(session=session)

    def delete_one(
        self,
        session: Optional[ClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        **pymongo_kwargs,
    ) -> DeleteOne:
        """
        Provide search criteria to the [DeleteOne](https://roman-right.github.io/beanie/api/queries/#deleteone) query
        :param session: Optional[ClientSession] - PyMongo sessions
        :return: [DeleteOne](https://roman-right.github.io/beanie/api/queries/#deleteone) query
        """
        return self.delete(
            session=session, bulk_writer=bulk_writer, **pymongo_kwargs
        )

    async def replace_one(
        self,
        document: "beanie.Document",
        session: Optional[ClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
    ) -> Optional[UpdateResult]:
        """
        Replace found document by provided
        :param document: Document - document, which will replace the found one
        :param session: Optional[ClientSession] - PyMongo session
        :param bulk_writer: Optional[BulkWriter] - Beanie bulk writer
        :return: UpdateResult
        """
        self.set_session(session=session)
        if bulk_writer is None:
            result: UpdateResult = (
                await self.document_model.get_motor_collection().replace_one(
                    self.get_filter_query(),
                    get_dict(
                        document,
                        to_db=True,
                        exclude={"_id"},
                        keep_nulls=document.get_settings().keep_nulls,
                    ),
                    session=self.session,
                )
            )

            if not result.raw_result["updatedExisting"]:
                raise DocumentNotFound
            return result
        else:
            bulk_writer.add_operation(
                Operation(
                    operation=ReplaceOne,
                    first_query=self.get_filter_query(),
                    second_query=Encoder(exclude={"_id"}).encode(document),
                    object_class=self.document_model,
                    pymongo_kwargs=self.pymongo_kwargs,
                )
            )
            return None

    async def _find_one(self):
        if self.fetch_links:
            return await self.document_model.find_many(
                *self.find_expressions,
                session=self.session,
                fetch_links=self.fetch_links,
                projection_model=self.projection_model,
                **self.pymongo_kwargs,
            ).first_or_none()
        return await self.document_model.get_motor_collection().find_one(
            filter=self.get_filter_query(),
            projection=self.get_projection(),
            session=self.session,
            **self.pymongo_kwargs,
        )

    def __await__(self) -> Generator[None, None, Optional[BaseModel]]:
        cache = self.cache
        if cache is not None:
            cache_key = self.cache_key
            document = cache.get(cache_key)
            if document is None:
                document = yield from self._find_one().__await__()
                cache.set(cache_key, document)
        else:
            document = yield from self._find_one().__await__()
        if document is None or isinstance(document, self.projection_model):
            return document
        return parse_obj(self.projection_model, document)

    async def count(self) -> int:
        """
        Count the number of documents matching the query
        :return: int
        """
        if self.fetch_links:
            return await self.document_model.find_many(
                *self.find_expressions,
                session=self.session,
                fetch_links=self.fetch_links,
                **self.pymongo_kwargs,
            ).count()
        return await super().count()
