from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel


class Granularity(str, Enum):
    """
    Time Series Granuality
    """

    seconds = "seconds"
    minutes = "minutes"
    hours = "hours"


class TimeSeriesConfig(BaseModel):
    """
    Time Series Collection config
    """

    time_field: str
    meta_field: Optional[str] = None
    granularity: Optional[Granularity] = None
    expire_after_seconds: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        res: Dict[str, Any] = {}
        timeseries = {"timeField": self.time_field}
        if self.meta_field is not None:
            timeseries["metaField"] = self.meta_field
        if self.granularity is not None:
            timeseries["granularity"] = self.granularity
        res["timeseries"] = timeseries
        if self.expire_after_seconds is not None:
            res["expireAfterSeconds"] = self.expire_after_seconds
        return res
