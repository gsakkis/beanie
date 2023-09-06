from datetime import timedelta
from typing import Any, Dict, Optional

from motor.motor_asyncio import AsyncIOMotorCollection
from pydantic import BaseModel, ConfigDict, Field
from typing_extensions import Self


class BaseSettings(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: Optional[str] = None
    class_id: str = "_class_id"

    use_cache: bool = False
    cache_capacity: int = 32
    cache_expiration_time: timedelta = timedelta(minutes=10)
    bson_encoders: Dict[Any, Any] = Field(default_factory=dict)
    projection: Optional[Dict[str, Any]] = None

    motor_collection: Optional[AsyncIOMotorCollection] = None

    @classmethod
    def from_model_type(cls, model_type: type) -> Self:
        self = cls.model_validate(
            model_type.Settings.__dict__
            if hasattr(model_type, "Settings")
            else {}
        )
        if self.name is None:
            self.name = model_type.__name__
        return self
