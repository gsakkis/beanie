import inspect
from typing import Any, Dict, List, Type, Union

from motor.motor_asyncio import AsyncIOMotorDatabase
from typing_extensions import Self

from beanie.odm.settings.base import ItemSettings


class ViewSettings(ItemSettings):
    source: Union[str, Type]
    pipeline: List[Dict[str, Any]]

    @classmethod
    def from_model_type(
        cls, model_type: type, database: AsyncIOMotorDatabase
    ) -> Self:
        settings = super().from_model_type(model_type, database)
        if inspect.isclass(settings.source):
            settings.source = settings.source.get_collection_name()
        return settings
