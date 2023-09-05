from typing import Any, List, Optional

from motor.motor_asyncio import AsyncIOMotorDatabase
from pydantic import ConfigDict, Field

import beanie
from beanie.exceptions import MongoDBVersionError
from beanie.odm.fields import IndexModelField
from beanie.odm.settings.base import ItemSettings
from beanie.odm.settings.timeseries import TimeSeriesConfig


class DocumentSettings(ItemSettings):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    use_state_management: bool = False
    state_management_replace_objects: bool = False
    state_management_save_previous: bool = False
    validate_on_save: bool = False
    use_revision: bool = False

    indexes: List[IndexModelField] = Field(default_factory=list)
    merge_indexes: bool = False
    timeseries: Optional[TimeSeriesConfig] = None

    keep_nulls: bool = True

    async def update_from_database(
        self, database: AsyncIOMotorDatabase, **kwargs: Any
    ) -> None:
        self.motor_collection = database[self.name]
        if self.timeseries:
            if beanie.DATABASE_MAJOR_VERSION < 5:
                raise MongoDBVersionError(
                    "Timeseries are supported by MongoDB version 5 and higher"
                )
            collections = await database.list_collection_names()
            if self.name and self.name not in collections:
                self.motor_collection = await database.create_collection(
                    **self.timeseries.build_query(self.name)
                )
