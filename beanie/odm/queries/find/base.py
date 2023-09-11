from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional, Type

from pydantic import BaseModel
from pymongo.client_session import ClientSession
from typing_extensions import Self

import beanie
from beanie.exceptions import NotSupported
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
        ignore_cache: bool = False,
        session: Optional[ClientSession] = None,
        **pymongo_kwargs: Any,
    ):
        super().__init__(session, **pymongo_kwargs)
        self.document_model = document_model
        self.projection_model = projection_model
        self.ignore_cache = ignore_cache
        self.fetch_links = False
        self.find_expressions: List[Mapping[str, Any]] = []

    def get_filter_query(self) -> Mapping[str, Any]:
        """Returns: MongoDB filter query"""
        if issubclass(self.document_model, LinkedModelMixin):
            for i, query in enumerate(self.find_expressions):
                self.find_expressions[i] = convert_ids(
                    query, self.document_model, self.fetch_links
                )
        if self.find_expressions:
            return Encoder(
                custom_encoders=self.document_model.get_settings().bson_encoders
            ).encode(And(*self.find_expressions))
        return {}

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
        if find_query := self.get_filter_query():
            pipeline.append({"$match": find_query})
        if extra_stages:
            pipeline += extra_stages
        if project and (projection := self._get_projection()) is not None:
            pipeline.append({"$project": projection})
        return pipeline


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


def convert_ids(
    query: Mapping[str, Any],
    model_type: Type[LinkedModelMixin],
    fetch_links: bool,
) -> Mapping[str, Any]:
    # TODO add all the cases
    new_query = {}
    for k, v in query.items():
        k_splitted = k.split(".")
        if (
            len(k_splitted) == 2
            and k_splitted[0] in model_type.get_link_fields()
            and k_splitted[1] == "id"
        ):
            k = ".".join((k_splitted[0], "_id" if fetch_links else "$id"))
        if isinstance(v, dict):
            v = convert_ids(v, model_type, fetch_links)
        new_query[k] = v
    return new_query
