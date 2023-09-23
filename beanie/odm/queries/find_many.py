from dataclasses import dataclass, field
from typing import (
    Any,
    Dict,
    List,
    Mapping,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    overload,
)

from motor.core import AgnosticBaseCursor
from pydantic import BaseModel
from pymongo.client_session import ClientSession
from typing_extensions import Self

import beanie
from beanie.exceptions import NotSupported
from beanie.odm.bulk import BulkWriter
from beanie.odm.fields import ExpressionField, SortDirection
from beanie.odm.interfaces.update import UpdateMethods
from beanie.odm.links import LinkedModelMixin
from beanie.odm.operators import FieldName, FieldNameMapping
from beanie.odm.queries.aggregation import AggregationQuery
from beanie.odm.queries.cursor import BaseCursorQuery, ProjectionT
from beanie.odm.queries.delete import DeleteMany
from beanie.odm.queries.find_query import FindQuery, get_projection
from beanie.odm.queries.update import UpdateMany
from beanie.odm.utils.parsing import ParseableModel

ModelT = TypeVar("ModelT", bound=BaseModel)


@dataclass
class FindMany(FindQuery, BaseCursorQuery[ProjectionT], UpdateMethods):
    """Find Many query class"""

    sort_expressions: List[Tuple[str, SortDirection]] = field(
        default_factory=list
    )
    skip_number: int = 0
    limit_number: int = 0

    def find(
        self,
        *args: FieldNameMapping,
        projection_model: Optional[Type[ParseableModel]] = None,
        skip: Optional[int] = None,
        limit: Optional[int] = None,
        sort: Union[
            None, FieldName, List[Tuple[FieldName, SortDirection]]
        ] = None,
        session: Optional[ClientSession] = None,
        ignore_cache: bool = False,
        fetch_links: bool = False,
        lazy_parse: bool = False,
        **pymongo_kwargs: Any,
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
        :keyword **pymongo_kwargs: pymongo native parameters for find operation.
            If Document class contains links, this parameter must fit the respective
            parameter of the aggregate MongoDB function.
        :return: FindMany - query instance
        """
        self.find_expressions.extend(map(ExpressionField.serialize, args))
        self.skip(skip)
        self.limit(limit)
        self.sort(sort)
        self.project(projection_model)
        self.set_session(session)
        self.ignore_cache = ignore_cache
        self.fetch_links = fetch_links
        self.pymongo_kwargs.update(pymongo_kwargs)
        self.lazy_parse = lazy_parse
        return self

    def sort(
        self,
        *args: Union[
            None,
            FieldName,
            Tuple[FieldName, SortDirection],
            List[Tuple[FieldName, SortDirection]],
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
            else:
                if isinstance(arg, tuple):
                    key, direction = arg
                else:
                    key = arg
                    direction = None
                self._add_sort(ExpressionField.serialize(key), direction)
        return self

    def _add_sort(self, key: str, direction: Optional[SortDirection]) -> None:
        if not isinstance(key, str):
            raise TypeError(f"Sort key must be a string, not {type(key)}")
        if direction is None:
            if key.startswith("-"):
                direction = SortDirection.DESCENDING
                key = key[1:]
            else:
                direction = SortDirection.ASCENDING
                if key.startswith("+"):
                    key = key[1:]

        self.sort_expressions.append((key, direction))

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
        *args: FieldNameMapping,
        session: Optional[ClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        **pymongo_kwargs: Any,
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
        return UpdateMany(
            document_model=self.document_model,
            find_query=self.get_filter_query(),
        ).update(
            *args,
            session=self.session,
            bulk_writer=bulk_writer,
            **pymongo_kwargs,
        )

    def upsert(
        self,
        *args: FieldNameMapping,
        on_insert: "beanie.Document",
        session: Optional[ClientSession] = None,
        **pymongo_kwargs: Any,
    ) -> UpdateMany:
        """
        Create Update with modifications query
        and provide search criteria there

        :param args: *Mapping[str,Any] - the modifications to apply.
        :param on_insert: Document - document to insert if there is no matched
        document in the collection
        :param session: Optional[ClientSession]
        :return: UpdateMany query
        """
        if not issubclass(self.document_model, beanie.Document):
            raise RuntimeError("can upsert only beanie.Document")
        self.set_session(session)
        return UpdateMany(
            document_model=self.document_model,
            find_query=self.get_filter_query(),
        ).update(
            *args, on_insert=on_insert, session=self.session, **pymongo_kwargs
        )

    def delete(
        self,
        bulk_writer: Optional[BulkWriter] = None,
        session: Optional[ClientSession] = None,
        **pymongo_kwargs: Any,
    ) -> DeleteMany:
        """
        Provide search criteria to the Delete query

        :param session: Optional[ClientSession]
        :return: Union[DeleteOne, DeleteMany]
        """
        self.set_session(session)
        return DeleteMany(
            document_model=self.document_model,
            find_query=self.get_filter_query(),
            bulk_writer=bulk_writer,
            session=self.session,
            pymongo_kwargs=pymongo_kwargs,
        )

    def _build_aggregation_pipeline(
        self,
        projection_model: Optional[Type[ParseableModel]],
        *aggregation_expressions: Mapping[str, Any],
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

        if aggregation_expressions:
            pipeline.extend(aggregation_expressions)
        if self.sort_expressions:
            pipeline.append({"$sort": dict(self.sort_expressions)})
        if self.skip_number != 0:
            pipeline.append({"$skip": self.skip_number})
        if self.limit_number != 0:
            pipeline.append({"$limit": self.limit_number})
        if (projection := get_projection(projection_model)) is not None:
            pipeline.append({"$project": projection})

        return pipeline

    @overload
    def aggregate(
        self,
        aggregation_pipeline: List[Any],
        projection_model: Type["ModelT"],
        session: Optional[ClientSession] = None,
        ignore_cache: bool = False,
        **pymongo_kwargs: Any,
    ) -> AggregationQuery["ModelT"]:
        ...

    @overload
    def aggregate(
        self,
        aggregation_pipeline: List[Any],
        projection_model: None = None,
        session: Optional[ClientSession] = None,
        ignore_cache: bool = False,
        **pymongo_kwargs: Any,
    ) -> AggregationQuery[Dict[str, Any]]:
        ...

    def aggregate(
        self,
        aggregation_pipeline: List[Any],
        projection_model: Optional[Type[ParseableModel]] = None,
        session: Optional[ClientSession] = None,
        ignore_cache: bool = False,
        **pymongo_kwargs: Any,
    ) -> Union[AggregationQuery["ModelT"], AggregationQuery[Dict[str, Any]]]:
        """
        Provide search criteria to the [AggregationQuery](query.md#aggregationquery)

        :param aggregation_pipeline: list - aggregation pipeline. MongoDB doc:
        <https://docs.mongodb.com/manual/core/aggregation-pipeline/>
        :param projection_model: Type[BaseModel] - Projection Model
        :param session: Optional[ClientSession] - PyMongo session
        :param ignore_cache: bool
        :return:[AggregationQuery](query.md#aggregationquery)
        """
        self.set_session(session)
        return AggregationQuery[Any](
            aggregation_pipeline=self._build_aggregation_pipeline(
                projection_model, *aggregation_pipeline
            ),
            document_model=self.document_model,
            projection_model=projection_model,
            cache_key_dict=self._cache_key_dict(),
            ignore_cache=ignore_cache,
            session=self.session,
            **pymongo_kwargs,
        )

    async def sum(
        self,
        field: str,
        session: Optional[ClientSession] = None,
        ignore_cache: bool = False,
        **pymongo_kwargs: Any,
    ) -> Optional[float]:
        """
        Sum of values of the given field

        Example:

        ```python

        class Sample(Document):
            price: int
            count: int

        sum_count = await Document.find(Sample.price <= 100).sum(Sample.count)

        ```

        :param field: str
        :param session: Optional[ClientSession] - pymongo session
        :param ignore_cache: bool
        :return: float - sum. None if there are no items.
        """
        return await self._compute_aggregate(
            "sum", field, session, ignore_cache, **pymongo_kwargs
        )

    async def avg(
        self,
        field: str,
        session: Optional[ClientSession] = None,
        ignore_cache: bool = False,
        **pymongo_kwargs: Any,
    ) -> Optional[float]:
        """
        Average of values of the given field

        Example:

        ```python

        class Sample(Document):
            price: int
            count: int

        avg_count = await Document.find(Sample.price <= 100).avg(Sample.count)
        ```

        :param field: str
        :param session: Optional[ClientSession] - pymongo session
        :param ignore_cache: bool
        :return: Optional[float] - avg. None if there are no items.
        """
        return await self._compute_aggregate(
            "avg", field, session, ignore_cache, **pymongo_kwargs
        )

    async def max(
        self,
        field: str,
        session: Optional[ClientSession] = None,
        ignore_cache: bool = False,
        **pymongo_kwargs: Any,
    ) -> Optional[float]:
        """
        Max of the values of the given field

        Example:

        ```python

        class Sample(Document):
            price: int
            count: int

        max_count = await Document.find(Sample.price <= 100).max(Sample.count)
        ```

        :param field: str
        :param session: Optional[ClientSession] - pymongo session
        :return: float - max. None if there are no items.
        """
        return await self._compute_aggregate(
            "max", field, session, ignore_cache, **pymongo_kwargs
        )

    async def min(
        self,
        field: str,
        session: Optional[ClientSession] = None,
        ignore_cache: bool = False,
        **pymongo_kwargs: Any,
    ) -> Optional[float]:
        """
        Min of the values of the given field

        Example:

        ```python

        class Sample(Document):
            price: int
            count: int

        min_count = await Document.find(Sample.price <= 100).min(Sample.count)
        ```

        :param field: str
        :param session: Optional[ClientSession] - pymongo session
        :return: float - min. None if there are no items.
        """
        return await self._compute_aggregate(
            "min", field, session, ignore_cache, **pymongo_kwargs
        )

    async def _compute_aggregate(
        self,
        operator: str,
        field: str,
        session: Optional[ClientSession] = None,
        ignore_cache: bool = False,
        **pymongo_kwargs: Any,
    ) -> Optional[float]:
        pipeline = [
            {"$group": {"_id": None, "value": {f"${operator}": f"${field}"}}},
            {"$project": {"_id": 0, "value": 1}},
        ]
        result = await self.aggregate(
            pipeline,
            session=session,
            ignore_cache=ignore_cache,
            **pymongo_kwargs,
        ).first_or_none()
        return result["value"] if result else None

    def _cache_key_dict(self) -> Dict[str, Any]:
        d = super()._cache_key_dict()
        d.update(
            sort=self.sort_expressions,
            skip=self.skip_number,
            limit=self.limit_number,
        )
        return d

    @property
    def _motor_cursor(self) -> AgnosticBaseCursor:
        if self.fetch_links:
            return self.document_model.get_motor_collection().aggregate(
                self._build_aggregation_pipeline(self.projection_model),
                session=self.session,
                **self.pymongo_kwargs,
            )

        return self.document_model.get_motor_collection().find(
            filter=self.get_filter_query(),
            sort=self.sort_expressions,
            projection=get_projection(self.projection_model),
            skip=self.skip_number,
            limit=self.limit_number,
            session=self.session,
            **self.pymongo_kwargs,
        )
