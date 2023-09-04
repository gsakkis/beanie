import dataclasses as dc
import datetime
import decimal
import enum
import ipaddress
import operator
import pathlib
import re
import uuid
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    Mapping,
    Optional,
    Set,
    Type,
)

import bson
import pydantic

import beanie
from beanie.odm.links import Link, LinkTypes

if TYPE_CHECKING:
    from pydantic.typing import TupleGenerator

    from beanie.odm.documents import DocType


DEFAULT_CUSTOM_ENCODERS: Dict[Type, Callable] = {
    ipaddress.IPv4Address: str,
    ipaddress.IPv4Interface: str,
    ipaddress.IPv4Network: str,
    ipaddress.IPv6Address: str,
    ipaddress.IPv6Interface: str,
    ipaddress.IPv6Network: str,
    pathlib.PurePath: str,
    pydantic.SecretBytes: pydantic.SecretBytes.get_secret_value,
    pydantic.SecretStr: pydantic.SecretStr.get_secret_value,
    datetime.timedelta: operator.methodcaller("total_seconds"),
    enum.Enum: operator.attrgetter("value"),
    Link: operator.attrgetter("ref"),
    bytes: bson.Binary,
    decimal.Decimal: bson.Decimal128,
    uuid.UUID: bson.Binary.from_uuid,
    re.Pattern: bson.Regex.from_native,
}
try:
    from pydantic_extra_types.color import Color
except ImportError:
    pass
else:
    DEFAULT_CUSTOM_ENCODERS[Color] = str


BSON_SCALAR_TYPES = (
    type(None),
    str,
    int,
    float,
    datetime.datetime,
    bson.Binary,
    bson.DBRef,
    bson.Decimal128,
    bson.ObjectId,
)
BACK_LINK_TYPES = frozenset(
    [
        LinkTypes.BACK_DIRECT,
        LinkTypes.BACK_LIST,
        LinkTypes.OPTIONAL_BACK_DIRECT,
        LinkTypes.OPTIONAL_BACK_LIST,
    ]
)


@dc.dataclass
class Encoder:
    """
    BSON encoding class
    """

    exclude: Set[str] = dc.field(default_factory=set)
    custom_encoders: Mapping[Type, Callable] = dc.field(default_factory=dict)
    to_db: bool = False
    keep_nulls: bool = True

    def __post_init__(self):
        # replace _id alias with id field name
        if "_id" in self.exclude and "id" not in self.exclude:
            self.exclude.add("id")
            self.exclude.remove("_id")

    def _encode_document(self, obj: "DocType") -> Mapping[str, Any]:
        obj.parse_store()
        settings = obj.get_settings()
        obj_dict: Dict[str, Any] = {}
        if settings.union_doc is not None:
            obj_dict[settings.class_id] = (
                settings.union_doc_alias or obj.__class__.__name__
            )
        if obj._inheritance_inited:
            obj_dict[settings.class_id] = obj._class_id

        link_fields = obj.get_link_fields() or {}
        sub_encoder = Encoder(
            # don't propagate self.exclude to subdocuments
            custom_encoders=settings.bson_encoders,
            to_db=self.to_db,
            keep_nulls=self.keep_nulls,
        )
        for key, value in self._iter_model_items(obj):
            if key in link_fields:
                link_type = link_fields[key].link_type
                if link_type is LinkTypes.DIRECT or (
                    link_type is LinkTypes.OPTIONAL_DIRECT
                    and value is not None
                ):
                    value = value.to_ref()
                elif link_type is LinkTypes.LIST or (
                    link_type is LinkTypes.OPTIONAL_LIST and value is not None
                ):
                    value = [link.to_ref() for link in value]
                elif link_type in BACK_LINK_TYPES and self.to_db:
                    continue
            obj_dict[key] = sub_encoder.encode(value)
        return obj_dict

    def _iter_model_items(self, obj: pydantic.BaseModel) -> "TupleGenerator":
        return obj._iter(
            by_alias=True,
            exclude=self.exclude,
            exclude_none=not self.keep_nulls,
        )

    def encode(self, obj: Any) -> Any:
        if self.custom_encoders:
            encoder = _get_encoder(obj, self.custom_encoders)
            if encoder is not None:
                return encoder(obj)

        if isinstance(obj, BSON_SCALAR_TYPES):
            return obj

        encoder = _get_encoder(obj, DEFAULT_CUSTOM_ENCODERS)
        if encoder is not None:
            return encoder(obj)

        if isinstance(obj, beanie.Document):
            return self._encode_document(obj)
        if isinstance(obj, pydantic.BaseModel):
            items = self._iter_model_items(obj)
            return {key: self.encode(value) for key, value in items}
        if isinstance(obj, Mapping):
            return {key: self.encode(value) for key, value in obj.items()}
        if isinstance(obj, Iterable):
            return [self.encode(value) for value in obj]

        errors = []
        try:
            data = dict(obj)
        except Exception as e:
            errors.append(e)
            try:
                data = vars(obj)
            except Exception as e:
                errors.append(e)
                raise ValueError(errors)
        return self.encode(data)


def _get_encoder(
    obj: Any, custom_encoders: Mapping[Type, Callable]
) -> Optional[Callable]:
    encoder = custom_encoders.get(type(obj))
    if encoder is not None:
        return encoder
    for cls, encoder in custom_encoders.items():
        if isinstance(obj, cls):
            return encoder
    return None
