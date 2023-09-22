from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping

from motor.core import AgnosticBaseCursor

from beanie.odm.queries.cursor import BaseCursorQuery, ProjectionT


@dataclass
class AggregationQuery(BaseCursorQuery[ProjectionT]):
    """Aggregation Query"""

    aggregation_pipeline: List[Mapping[str, Any]] = field(default_factory=list)
    cache_key_dict: Dict[str, Any] = field(default_factory=dict)

    def _cache_key_dict(self) -> Dict[str, Any]:
        return dict(self.cache_key_dict, pipeline=self.aggregation_pipeline)

    @property
    def _motor_cursor(self) -> AgnosticBaseCursor:
        return self.document_model.get_motor_collection().aggregate(
            self.aggregation_pipeline,
            session=self.session,
            **self.pymongo_kwargs,
        )
