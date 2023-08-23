from typing import List, Optional

from pydantic import ConfigDict, Field

from beanie.odm.fields import IndexModelField
from beanie.odm.settings.base import ItemSettings
from beanie.odm.settings.timeseries import TimeSeriesConfig


class DocumentSettings(ItemSettings):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    use_state_management: bool = False
    state_management_replace_objects: bool = False
    state_management_save_previous: bool = False
    validate_on_save: bool = False
    use_revision: bool = False

    indexes: List[IndexModelField] = Field(default_factory=list)
    merge_indexes: bool = False
    timeseries: Optional[TimeSeriesConfig] = None

    keep_nulls: bool = True
