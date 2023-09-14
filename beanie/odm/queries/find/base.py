from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional, Type

from pydantic import BaseModel
from pymongo.client_session import ClientSession
from typing_extensions import Self

import beanie
from beanie.odm.cache import LRUCache
from beanie.odm.links import LinkedModelMixin
from beanie.odm.operators.logical import And
from beanie.odm.queries import BaseQuery
from beanie.odm.utils.encoder import Encoder
from beanie.odm.utils.parsing import ParseableModel

if TYPE_CHECKING:
    from beanie.odm.interfaces.find import FindInterface


class FindQuery(BaseQuery):
    """Find Query base class"""

    _caches: Dict[type, LRUCache] = {}

    def __init__(
        self,
        document_model: Type["FindInterface"],
        projection_model: Optional[Type[ParseableModel]] = None,
        session: Optional[ClientSession] = None,
        ignore_cache: bool = False,
        fetch_links: bool = False,
        **pymongo_kwargs: Any,
    ):
        super().__init__(session, **pymongo_kwargs)
        bson_encoders = document_model.get_settings().bson_encoders
        self.encoder = Encoder(custom_encoders=bson_encoders)
        self.document_model = document_model
        self.projection_model = projection_model
        self.ignore_cache = ignore_cache
        self.fetch_links = fetch_links
        self.find_expressions: List[Mapping[str, Any]] = []

    def get_filter_query(self) -> Mapping[str, Any]:
        """Returns: MongoDB filter query"""
        expressions = self.find_expressions
        for i, expression in enumerate(expressions):
            expressions[i] = self._convert_ids(expression)
        return self.encoder.encode(And(*expressions)) if expressions else {}

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
        if self.fetch_links:
            from .many import AggregationQuery

            result = await AggregationQuery[Dict[str, int]](
                *self.find_expressions,
                aggregation_pipeline=[{"$count": "count"}],
                document_model=self.document_model,
                session=self.session,
                ignore_cache=self.ignore_cache,
                fetch_links=self.fetch_links,
                **self.pymongo_kwargs,
            ).to_list(length=1)
            return result[0]["count"] if result else 0

        collection = self.document_model.get_motor_collection()
        return await collection.count_documents(self.get_filter_query())

    async def exists(self) -> bool:
        """
        If find query will return anything

        :return: bool
        """
        return await self.count() > 0

    @property
    def _cache(self) -> Optional[LRUCache]:
        if self.ignore_cache:
            return None
        settings = self.document_model.get_settings()
        if not settings.use_cache:
            return None
        try:
            return self._caches[self.document_model]
        except KeyError:
            cache = LRUCache(
                capacity=settings.cache_capacity,
                expiration_time=settings.cache_expiration_time,
            )
            return self._caches.setdefault(self.document_model, cache)

    @property
    def _cache_key(self) -> str:
        return str(self._cache_key_dict())

    def _cache_key_dict(self) -> Dict[str, Any]:
        return dict(
            type=self.__class__.__name__,
            filter=self.get_filter_query(),
            projection=self._get_projection(),
            fetch_links=self.fetch_links,
        )

    def _get_projection(self) -> Optional[Mapping[str, Any]]:
        if self.projection_model is None or not issubclass(
            self.projection_model, BaseModel
        ):
            return None
        return get_projection(self.projection_model)

    def _convert_ids(self, expression: Mapping[str, Any]) -> Mapping[str, Any]:
        if not issubclass(self.document_model, LinkedModelMixin):
            return expression

        # TODO add all the cases
        new_query = {}
        for k, v in expression.items():
            ksplit = k.split(".")
            if (
                len(ksplit) == 2
                and ksplit[0] in self.document_model.get_link_fields()
                and ksplit[1] == "id"
            ):
                k = ".".join((ksplit[0], "_id" if self.fetch_links else "$id"))
            new_query[k] = self._convert_ids(v) if isinstance(v, dict) else v
        return new_query


def get_projection(model: Type[BaseModel]) -> Optional[Mapping[str, Any]]:
    if issubclass(model, beanie.Document) and model._class_id:
        return None

    if hasattr(model, "Settings"):  # MyPy checks
        settings = getattr(model, "Settings")
        if hasattr(settings, "projection"):
            return getattr(settings, "projection")

    if model.model_config.get("extra") == "allow":
        return None

    return {
        field.alias or name: 1 for name, field in model.model_fields.items()
    }
