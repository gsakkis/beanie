from typing import Any, Dict, List, Mapping, Optional, Type

from pydantic import BaseModel
from typing_extensions import Self

import beanie
from beanie.odm.interfaces.settings import SettingsInterface
from beanie.odm.links import LinkedModelMixin
from beanie.odm.operators.logical import And
from beanie.odm.queries import BaseQuery, Cacheable
from beanie.odm.utils.encoder import Encoder
from beanie.odm.utils.parsing import ParseableModel


class FindQuery(BaseQuery, Cacheable):
    """Find Query base class"""

    def __init__(
        self,
        document_model: Type[SettingsInterface],
        projection_model: Optional[Type[ParseableModel]] = None,
    ):
        BaseQuery.__init__(self)
        bson_encoders = document_model.get_settings().bson_encoders
        self.encoder = Encoder(custom_encoders=bson_encoders)
        self.document_model = document_model
        self.projection_model = projection_model
        self.fetch_links = False
        self.ignore_cache = False
        self.find_expressions: List[Mapping[str, Any]] = []

    def get_filter_query(self) -> Dict[str, Any]:
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
            from beanie.odm.queries.find_many import FindMany

            query = (
                FindMany(self.document_model)
                .find(*self.find_expressions, fetch_links=self.fetch_links)
                .aggregate(
                    [{"$count": "count"}],
                    session=self.session,
                    ignore_cache=self.ignore_cache,
                    **self.pymongo_kwargs,
                )
            )
            result = await query.first_or_none()
            return result["count"] if result else 0

        collection = self.document_model.get_motor_collection()
        return await collection.count_documents(self.get_filter_query())

    async def exists(self) -> bool:
        """
        If find query will return anything

        :return: bool
        """
        return await self.count() > 0

    def _cache_key_dict(self) -> Dict[str, Any]:
        return dict(
            type=self.__class__.__name__,
            filter=self.get_filter_query(),
            projection=get_projection(self.projection_model),
            fetch_links=self.fetch_links,
        )

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


def get_projection(
    model: Optional[Type[ParseableModel]],
) -> Optional[Mapping[str, Any]]:
    if model is None or not issubclass(model, BaseModel):
        return None

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
