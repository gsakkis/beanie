import inspect
from typing import Any, Dict, List, Type, Union

from motor.motor_asyncio import AsyncIOMotorDatabase
from typing_extensions import Self

from beanie.odm.settings.base import ItemSettings


class ViewSettings(ItemSettings):
    source: Union[str, Type]
    pipeline: List[Dict[str, Any]]

    async def update_from_database(
        self, database: AsyncIOMotorDatabase, recreate: bool = False, **_: Any
    ) -> None:
        self.motor_collection = database[self.name]
        collection_names = await database.list_collection_names()
        if recreate or self.name not in collection_names:
            if self.name in collection_names:
                await self.motor_collection.drop()
            await database.command(
                {
                    "create": self.name,
                    "viewOn": self.source,
                    "pipeline": self.pipeline,
                }
            )

    @classmethod
    def from_model_type(cls, model_type: type) -> Self:
        settings = super().from_model_type(model_type)
        if inspect.isclass(settings.source):
            settings.source = settings.source.get_collection_name()
        return settings
