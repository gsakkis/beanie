from abc import abstractmethod
from typing import Any, Dict, Optional, Type, Union, overload

from pymongo.client_session import ClientSession

from beanie.odm.queries.aggregation import AggregationQuery
from beanie.odm.queries.find import FindMany, FindQueryProjectionType


class AggregateInterface:
    @classmethod
    @abstractmethod
    def find_all(cls) -> FindMany:
        pass

    @overload
    @classmethod
    def aggregate(
        cls,
        aggregation_pipeline: list,
        projection_model: None = None,
        session: Optional[ClientSession] = None,
        ignore_cache: bool = False,
        **pymongo_kwargs,
    ) -> AggregationQuery[Dict[str, Any]]:
        ...

    @overload
    @classmethod
    def aggregate(
        cls,
        aggregation_pipeline: list,
        projection_model: Type[FindQueryProjectionType],
        session: Optional[ClientSession] = None,
        ignore_cache: bool = False,
        **pymongo_kwargs,
    ) -> AggregationQuery[FindQueryProjectionType]:
        ...

    @classmethod
    def aggregate(
        cls,
        aggregation_pipeline: list,
        projection_model: Optional[Type[FindQueryProjectionType]] = None,
        session: Optional[ClientSession] = None,
        ignore_cache: bool = False,
        **pymongo_kwargs,
    ) -> Union[
        AggregationQuery[Dict[str, Any]],
        AggregationQuery[FindQueryProjectionType],
    ]:
        """
        Aggregate over collection.
        Returns [AggregationQuery](https://roman-right.github.io/beanie/api/queries/#aggregationquery) query object
        :param aggregation_pipeline: list - aggregation pipeline
        :param projection_model: Type[BaseModel]
        :param session: Optional[ClientSession]
        :param ignore_cache: bool
        :param **pymongo_kwargs: pymongo native parameters for aggregate operation
        :return: [AggregationQuery](https://roman-right.github.io/beanie/api/queries/#aggregationquery)
        """
        return cls.find_all().aggregate(
            aggregation_pipeline=aggregation_pipeline,
            projection_model=projection_model,
            session=session,
            ignore_cache=ignore_cache,
            **pymongo_kwargs,
        )
