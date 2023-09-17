from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional, Type

from motor.core import AgnosticBaseCursor
from pymongo.client_session import ClientSession

from beanie.odm.queries import BaseQuery
from beanie.odm.queries.cursor import BaseCursorQuery, ProjectionT
from beanie.odm.utils.parsing import ParseableModel

if TYPE_CHECKING:
    from beanie.odm.interfaces.find import FindInterface


class AggregationQuery(BaseCursorQuery[ProjectionT], BaseQuery):
    """Aggregation Query"""

    def __init__(
        self,
        aggregation_pipeline: List[Mapping[str, Any]],
        document_model: Type["FindInterface"],
        projection_model: Optional[Type[ParseableModel]],
        cache_key_dict: Dict[str, Any],
        ignore_cache: bool = False,
        session: Optional[ClientSession] = None,
        **pymongo_kwargs: Any,
    ):
        BaseQuery.__init__(self, session, **pymongo_kwargs)
        BaseCursorQuery.__init__(
            self, document_model, projection_model, ignore_cache
        )
        self.aggregation_pipeline = aggregation_pipeline
        self.__cache_key_dict = dict(
            cache_key_dict, pipeline=aggregation_pipeline
        )

    def _cache_key_dict(self) -> Dict[str, Any]:
        return self.__cache_key_dict

    @property
    def _motor_cursor(self) -> AgnosticBaseCursor:
        return self.document_model.get_motor_collection().aggregate(
            self.aggregation_pipeline,
            session=self.session,
            **self.pymongo_kwargs,
        )
