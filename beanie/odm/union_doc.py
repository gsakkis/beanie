from typing import ClassVar, Dict, Type

from motor.motor_asyncio import AsyncIOMotorDatabase

from beanie.odm.interfaces.find import BaseSettings, FindInterface


class UnionDocSettings(BaseSettings):
    pass


class UnionDoc(FindInterface):
    _children: ClassVar[Dict[str, Type]]
    _settings: ClassVar[UnionDocSettings]

    @classmethod
    async def update_from_database(
        cls, database: AsyncIOMotorDatabase
    ) -> None:
        cls._children = {}
        cls._settings = UnionDocSettings.from_model_type(cls, database)

    @classmethod
    def get_settings(cls) -> UnionDocSettings:
        return cls._settings
