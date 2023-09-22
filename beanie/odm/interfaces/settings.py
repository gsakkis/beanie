from datetime import timedelta
from typing import (
    Any,
    ClassVar,
    Generic,
    Mapping,
    Optional,
    Type,
    TypeVar,
    cast,
)

from motor.motor_asyncio import AsyncIOMotorCollection, AsyncIOMotorDatabase
from pydantic import BaseModel, ConfigDict, Field, model_validator
from typing_extensions import Self

from beanie.odm.cache import LRUCache


class BaseSettings(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str
    database: AsyncIOMotorDatabase
    class_id: str = "_class_id"

    cache: Optional[LRUCache[str, Any]] = None
    use_cache: bool = False
    cache_capacity: int = 32
    cache_expiration_time: timedelta = timedelta(minutes=10)
    bson_encoders: Mapping[Any, Any] = Field(default_factory=dict)

    @property
    def motor_collection(self) -> AsyncIOMotorCollection:
        return self.database[self.name]

    @model_validator(mode="after")
    def _init_cache(self) -> Self:
        if self.use_cache:
            self.cache = LRUCache(
                capacity=self.cache_capacity,
                expiration_time=self.cache_expiration_time,
            )
        return self


SettingsT = TypeVar("SettingsT", bound=BaseSettings, covariant=True)


class SettingsInterface(Generic[SettingsT]):
    # The concrete type of `SettingsT` to be defined in subclasses. Unfortunately this
    # need to be duplicated both in the base class and as a ClassVar:
    # class Foo(FindInterface[FooSettings]):
    #     _settings_type = FooSettings
    _settings_type: ClassVar[Type[BaseSettings]]

    # Should be ClassVar[SettingsT] but ClassVars cannot contain type variable
    _settings: ClassVar[BaseSettings]

    @classmethod
    def set_settings(cls, database: AsyncIOMotorDatabase) -> None:
        settings = dict(name=cls.__name__, database=database)
        if hasattr(cls, "Settings"):
            settings.update(vars(cls.Settings))
        cls._settings = cls._settings_type.model_validate(settings)

    @classmethod
    def get_settings(cls) -> SettingsT:
        return cast(SettingsT, cls._settings)

    @classmethod
    def get_motor_collection(cls) -> AsyncIOMotorCollection:
        return cls.get_settings().motor_collection

    @classmethod
    def get_collection_name(cls) -> str:
        return cls.get_settings().name

    @classmethod
    def get_cache(cls) -> Optional[LRUCache[str, Any]]:
        return cls.get_settings().cache
