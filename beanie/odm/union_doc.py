from typing import ClassVar, Dict

from motor.motor_asyncio import AsyncIOMotorDatabase

from beanie.odm.interfaces.find import FindInterface
from beanie.odm.interfaces.settings import BaseSettings, SettingsInterface


class UnionDoc(SettingsInterface[BaseSettings], FindInterface):
    _settings_type = BaseSettings
    _children: ClassVar[Dict[str, type]]

    @classmethod
    def init_from_database(cls, database: AsyncIOMotorDatabase) -> None:
        cls._children = {}
        cls.set_settings(database)
