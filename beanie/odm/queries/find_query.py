from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Type

from pydantic import BaseModel
from typing_extensions import Self

import beanie
from beanie.odm.links import LinkedModelMixin
from beanie.odm.operators.logical import And
from beanie.odm.queries import CacheableQuery
from beanie.odm.utils.parsing import ParseableModel


@dataclass
class FindQuery(CacheableQuery):
    """Find Query base class"""

    projection_model: Optional[Type[ParseableModel]] = None
    fetch_links: bool = False
    find_expressions: List[Mapping[str, Any]] = field(default_factory=list)

    def get_filter_query(self) -> Dict[str, Any]:
        """Returns: MongoDB filter query"""
        expressions = self.find_expressions
        if not expressions:
            return {}
        for i, expression in enumerate(expressions):
            expressions[i] = self._convert_ids(expression)
        expression = And(*expressions)
        return {k: self.encoder.encode(v) for k, v in expression.items()}

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
            result: Optional[Dict[str, int]] = await query.first_or_none()
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
        projection = getattr(settings, "projection", None)
        if isinstance(projection, dict):
            return projection

    if model.model_config.get("extra") == "allow":
        return None

    return {
        field.alias or name: 1 for name, field in model.model_fields.items()
    }
