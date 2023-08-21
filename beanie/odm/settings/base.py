from datetime import timedelta
from typing import Any, Dict, Optional, get_args, get_origin

import msgspec
from motor.motor_asyncio import AsyncIOMotorCollection
from typing_extensions import Self


class ItemSettings(msgspec.Struct, kw_only=True):
    name: Optional[str] = None

    use_cache: bool = False
    cache_capacity: int = 32
    cache_expiration_time: timedelta = timedelta(minutes=10)
    bson_encoders: Dict[Any, Any] = {}
    projection: Optional[Dict[str, Any]] = None

    motor_collection: Optional[AsyncIOMotorCollection] = None

    union_doc: Optional[type] = None
    union_doc_alias: Optional[str] = None
    class_id: str = "_class_id"

    is_root: bool = False

    @classmethod
    def model_validate(cls, settings: Dict[str, Any]) -> Self:
        return msgspec.convert(settings, cls, dec_hook=cls._decode_hook)

    @classmethod
    def _decode_hook(cls, annotation, obj):
        # TODO: remove/simplify after https://github.com/jcrist/msgspec/issues/529
        origin_type = get_origin(annotation) or annotation
        if origin_type is type and isinstance(obj, type):
            args = get_args(annotation)
            if not args or issubclass(obj, args[0]):
                return obj
        raise TypeError(f"Cannot decode {obj} to {annotation}")
