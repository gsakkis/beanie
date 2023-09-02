from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional, Type

from pydantic import BaseModel
from pymongo.client_session import ClientSession
from typing_extensions import Self

import beanie
from beanie.odm.cache import LRUCache
from beanie.odm.fields import ExpressionField
from beanie.odm.operators.find.logical import And
from beanie.odm.queries import BaseQuery
from beanie.odm.utils.encoder import Encoder
from beanie.odm.utils.parsing import ParseableModel

if TYPE_CHECKING:
    from beanie.odm.interfaces.find import FindInterface


class FindQuery(BaseQuery):
    """Find Query base class"""

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
        self.find_expressions: List[Dict[str, Any]] = []

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
        if (
            not self.ignore_cache
            and self.document_model.get_settings().use_cache
            and issubclass(self.document_model, beanie.Document)
        ):
            return self.document_model._cache
        return None

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

    def _get_projection(self) -> Optional[Dict[str, Any]]:
        if self.projection_model is None or not issubclass(
            self.projection_model, BaseModel
        ):
            return None
        return get_projection(self.projection_model)


def get_projection(model: Type[BaseModel]) -> Optional[Dict[str, Any]]:
    if issubclass(model, beanie.Document) and model._inheritance_inited:
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
    query: Dict[str, Any], model_type: Type["FindInterface"], fetch_links: bool
) -> Dict[str, Any]:
    # TODO add all the cases
    new_query = {}
    for k, v in query.items():
        k_splitted = k.split(".")
        if (
            isinstance(k, ExpressionField)
            and model_type.get_link_fields() is not None
            and len(k_splitted) == 2
            and k_splitted[0] in model_type.get_link_fields()
            and k_splitted[1] == "id"
        ):
            if fetch_links:
                new_k = f"{k_splitted[0]}._id"
            else:
                new_k = f"{k_splitted[0]}.$id"
        else:
            new_k = k

        if isinstance(v, dict):
            new_v = convert_ids(v, model_type, fetch_links)
        else:
            new_v = v

        new_query[new_k] = new_v
    return new_query
