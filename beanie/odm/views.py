from typing import Any, Dict, List

from motor.motor_asyncio import AsyncIOMotorDatabase
from pydantic import BaseModel, field_validator

from beanie.odm.interfaces.find import FindInterface
from beanie.odm.interfaces.settings import BaseSettings
from beanie.odm.links import LinkedModelMixin


class ViewSettings(BaseSettings):
    source: str
    pipeline: List[Dict[str, Any]]

    @field_validator("source", mode="before")
    @classmethod
    def _source_name(cls, v: Any) -> Any:
        return v.get_collection_name() if issubclass(v, FindInterface) else v


class View(BaseModel, LinkedModelMixin, FindInterface[ViewSettings]):
    _settings_type = ViewSettings

    @classmethod
    async def update_from_database(
        cls, database: AsyncIOMotorDatabase
    ) -> None:
        cls.set_settings(database)
