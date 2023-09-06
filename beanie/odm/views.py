from typing import Any, ClassVar, Dict, List

from motor.motor_asyncio import AsyncIOMotorDatabase
from pydantic import field_validator

from beanie.odm.interfaces.find import BaseSettings, FindInterface
from beanie.odm.links import LinkedModel


class ViewSettings(BaseSettings):
    source: str
    pipeline: List[Dict[str, Any]]

    @field_validator("source", mode="before")
    @classmethod
    def _source_name(cls, v: Any) -> Any:
        return v.get_collection_name() if issubclass(v, FindInterface) else v

    async def update_from_database(
        self, database: AsyncIOMotorDatabase, recreate: bool = False
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


class View(LinkedModel, FindInterface):
    _settings: ClassVar[ViewSettings]

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
        super().__pydantic_init_subclass__(**kwargs)
        cls._settings = ViewSettings.from_model_type(cls)

    @classmethod
    def get_settings(cls) -> ViewSettings:
        return cls._settings
