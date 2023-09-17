from typing import ClassVar, Dict, Type

from motor.motor_asyncio import AsyncIOMotorDatabase

from beanie.odm.interfaces.find import BaseSettings, FindInterface


class UnionDoc(FindInterface[BaseSettings]):
    _settings_type = BaseSettings
    _children: ClassVar[Dict[str, Type]]

    @classmethod
    async def update_from_database(
        cls, database: AsyncIOMotorDatabase
    ) -> None:
        cls._children = {}
        cls.set_settings(database)
