import asyncio
from typing import Any, Dict, List, Union

from motor.motor_asyncio import AsyncIOMotorDatabase
from pydantic import field_validator

from beanie.odm.interfaces.find import FindInterface
from beanie.odm.links import Link, LinkedModel
from beanie.odm.settings import BaseSettings


class ViewSettings(BaseSettings):
    source: str
    pipeline: List[Dict[str, Any]]

    @field_validator("source", mode="before")
    @classmethod
    def _source_name(cls, v: Any) -> Any:
        return v.get_collection_name() if issubclass(v, FindInterface) else v

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


class View(LinkedModel, FindInterface):
    async def fetch_link(self, field: Union[str, Any]):
        ref_obj = getattr(self, field, None)
        if isinstance(ref_obj, Link):
            value = await ref_obj.fetch(fetch_links=True)
            setattr(self, field, value)
        if isinstance(ref_obj, list) and ref_obj:
            values = await Link.fetch_list(ref_obj, fetch_links=True)
            setattr(self, field, values)

    async def fetch_all_links(self):
        coros = []
        link_fields = self.get_link_fields()
        if link_fields is not None:
            for ref in link_fields.values():
                coros.append(self.fetch_link(ref.field_name))  # TODO lists
        await asyncio.gather(*coros)
