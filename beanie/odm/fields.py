from enum import Enum
from typing import Any, List

import bson
import pymongo
from bson.errors import InvalidId
from pydantic import PlainSerializer, PlainValidator, WithJsonSchema
from pydantic_core import core_schema
from typing_extensions import Annotated

from beanie.odm.operators.find.comparison import GT, GTE, LT, LTE, NE, Eq


def Indexed(annotation, index_type=pymongo.ASCENDING, **kwargs):
    """
    Returns an Annotated type with a `{"get_index_model" : f}` dict metadata, where
    f is a function `f(key) -> IndexModel` that generates a pymongo Index instance
    for the given key.
    """

    def get_index_model(key):
        return pymongo.IndexModel([(key, index_type)], **kwargs)

    return Annotated[annotation, {"get_index_model": get_index_model}]


def _validate_objectid(v: Any) -> bson.ObjectId:
    try:
        return bson.ObjectId(v.decode("utf-8") if isinstance(v, bytes) else v)
    except InvalidId:
        raise ValueError("Id must be of type bson.ObjectId")


PydanticObjectId = Annotated[
    bson.ObjectId,
    PlainValidator(_validate_objectid),
    PlainSerializer(lambda v: str(v)),
    WithJsonSchema({"type": "string", "example": "5eb7cf5a86d9755df3a6c593"}),
]


class SortDirection(int, Enum):
    """Sorting directions"""

    ASCENDING = pymongo.ASCENDING
    DESCENDING = pymongo.DESCENDING


class ExpressionField(str):
    def __getitem__(self, item):
        """
        Get sub field

        :param item: name of the subfield
        :return: ExpressionField
        """
        return ExpressionField(f"{self}.{item}")

    def __getattr__(self, item):
        """
        Get sub field

        :param item: name of the subfield
        :return: ExpressionField
        """
        return ExpressionField(f"{self}.{item}")

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        if isinstance(other, ExpressionField):
            return super(ExpressionField, self).__eq__(other)
        return Eq(field=self, other=other)

    def __gt__(self, other):
        return GT(field=self, other=other)

    def __ge__(self, other):
        return GTE(field=self, other=other)

    def __lt__(self, other):
        return LT(field=self, other=other)

    def __le__(self, other):
        return LTE(field=self, other=other)

    def __ne__(self, other):
        return NE(field=self, other=other)

    def __pos__(self):
        return self, SortDirection.ASCENDING

    def __neg__(self):
        return self, SortDirection.DESCENDING

    def __copy__(self):
        return self

    def __deepcopy__(self, memo):
        return self


class IndexModelField:
    def __init__(self, index: pymongo.IndexModel):
        self.index = index
        self.name = index.document["name"]

        self.fields = tuple(sorted(self.index.document["key"]))
        self.options = tuple(
            sorted(
                (k, v)
                for k, v in self.index.document.items()
                if k not in ["key", "v"]
            )
        )

    def __eq__(self, other):
        return self.fields == other.fields and self.options == other.options

    def __repr__(self):
        return f"IndexModelField({self.name}, {self.fields}, {self.options})"

    @classmethod
    def from_motor_index_information(cls, index_info: dict):
        result = []
        for name, details in index_info.items():
            fields = details["key"]
            if ("_id", 1) in fields:
                continue

            options = {k: v for k, v in details.items() if k != "key"}
            index_model = IndexModelField(
                pymongo.IndexModel(fields, name=name, **options)
            )
            result.append(index_model)
        return result

    @staticmethod
    def merge_indexes(
        left: List["IndexModelField"], right: List["IndexModelField"]
    ):
        left_dict = {index.fields: index for index in left}
        right_dict = {index.fields: index for index in right}
        left_dict.update(right_dict)
        return list(left_dict.values())

    @classmethod
    def __get_pydantic_core_schema__(cls, source_type, handler):
        def validate(v, _):
            if not isinstance(v, pymongo.IndexModel):
                v = pymongo.IndexModel(v)
            return IndexModelField(v)

        return core_schema.general_plain_validator_function(validate)
