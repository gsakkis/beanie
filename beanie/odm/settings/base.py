from datetime import timedelta
from typing import Any, Dict, Optional, Type

from motor.motor_asyncio import AsyncIOMotorCollection, AsyncIOMotorDatabase
from pydantic import BaseModel, ConfigDict, Field
from typing_extensions import Self


class ItemSettings(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: Optional[str] = None

    use_cache: bool = False
    cache_capacity: int = 32
    cache_expiration_time: timedelta = timedelta(minutes=10)
    bson_encoders: Dict[Any, Any] = Field(default_factory=dict)
    projection: Optional[Dict[str, Any]] = None

    motor_collection: Optional[AsyncIOMotorCollection] = None

    union_doc: Optional[Type] = None
    union_doc_alias: Optional[str] = None
    class_id: str = "_class_id"

    is_root: bool = False

    async def update_from_database(
        self, database: AsyncIOMotorDatabase, **kwargs: Any
    ) -> None:
        self.motor_collection = database[self.name]

    @classmethod
    def from_model_type(cls, model_type: type) -> Self:
        settings = cls.model_validate(
            model_type.Settings.__dict__
            if hasattr(model_type, "Settings")
            else {}
        )
        if settings.name is None:
            settings.name = model_type.__name__
        return settings
