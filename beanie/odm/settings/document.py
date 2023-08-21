from typing import List, Optional

from pymongo import IndexModel

from beanie.odm.fields import IndexModelField
from beanie.odm.settings.base import ItemSettings
from beanie.odm.settings.timeseries import TimeSeriesConfig


class DocumentSettings(ItemSettings):
    use_state_management: bool = False
    state_management_replace_objects: bool = False
    state_management_save_previous: bool = False
    validate_on_save: bool = False
    use_revision: bool = False

    indexes: List[IndexModelField] = []
    merge_indexes: bool = False
    timeseries: Optional[TimeSeriesConfig] = None

    keep_nulls: bool = True

    @classmethod
    def _decode_hook(cls, annotation, obj):
        if annotation is IndexModelField:
            if not isinstance(obj, IndexModel):
                obj = IndexModel(obj)
            return IndexModelField(obj)
        return super()._decode_hook(annotation, obj)
