from typing import ClassVar, Dict, Type

from motor.motor_asyncio import AsyncIOMotorDatabase

from beanie.odm.interfaces.find import FindInterface
from beanie.odm.settings import BaseSettings


class UnionDocSettings(BaseSettings):
    async def update_from_database(
        self, database: AsyncIOMotorDatabase
    ) -> None:
        self.motor_collection = database[self.name]


class UnionDoc(FindInterface[UnionDocSettings]):
    _children: ClassVar[Dict[str, Type]]
    _settings: ClassVar[UnionDocSettings]

    def __init_subclass__(cls):
        cls._children = {}
        cls._settings = UnionDocSettings.from_model_type(cls)

    @classmethod
    def get_settings(cls) -> UnionDocSettings:
        return cls._settings
