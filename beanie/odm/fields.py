from enum import Enum
from functools import cached_property
from typing import TYPE_CHECKING, Any, Iterator, List, Mapping, Tuple, Union

import bson
import pymongo
from bson.errors import InvalidId
from pydantic import (
    GetCoreSchemaHandler,
    PlainSerializer,
    PlainValidator,
    WithJsonSchema,
)
from pydantic_core import core_schema
from typing_extensions import Annotated, Self

from beanie.odm.operators import BaseOperator, comparison

if TYPE_CHECKING:
    from types import GenericAlias


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


class ExpressionField:
    __slots__ = ("_expr",)

    def __init__(self, expression: str):
        self._expr = expression

    def __str__(self) -> str:
        return self._expr

    def __getitem__(self, item: str) -> Self:
        """
        Get sub field

        :param item: name of the subfield
        :return: ExpressionField
        """
        return self.__class__(f"{self._expr}.{item}")

    __getattr__ = __getitem__

    def __hash__(self) -> int:
        return hash(self._expr)

    def __eq__(self, other: Any) -> Union[BaseOperator, bool]:  # type: ignore[override]
        if isinstance(other, ExpressionField):
            return self._expr == other._expr
        return comparison.Eq(self._expr, other)

    def __ne__(self, other: Any) -> Union[BaseOperator, bool]:  # type: ignore[override]
        if isinstance(other, ExpressionField):
            return self._expr != other._expr
        return comparison.NE(self._expr, other)

    def __gt__(self, other: Any) -> BaseOperator:
        return comparison.GT(self._expr, other)

    def __ge__(self, other: Any) -> BaseOperator:
        return comparison.GTE(self._expr, other)

    def __lt__(self, other: Any) -> BaseOperator:
        return comparison.LT(self._expr, other)

    def __le__(self, other: Any) -> BaseOperator:
        return comparison.LTE(self._expr, other)

    def __pos__(self) -> Tuple[str, SortDirection]:
        return self._expr, SortDirection.ASCENDING

    def __neg__(self) -> Tuple[str, SortDirection]:
        return self._expr, SortDirection.DESCENDING

    def __copy__(self) -> Self:
        return self

    def __deepcopy__(self, memo: Mapping[int, Any]) -> Self:
        return self

    @classmethod
    def serialize(cls, expression: Any) -> Any:
        if isinstance(expression, Mapping):
            return {
                cls.serialize(k): cls.serialize(v)
                for k, v in expression.items()
            }
        if isinstance(expression, list):
            return list(map(cls.serialize, expression))
        if isinstance(expression, ExpressionField):
            return str(expression)
        return expression


class IndexModel(pymongo.IndexModel):
    @classmethod
    def from_pymongo(cls, index: pymongo.IndexModel) -> Self:
        self = cls.__new__(cls)
        self.__document = index.document
        return self

    @cached_property
    def name(self) -> str:
        return str(self.document["name"])

    @cached_property
    def keys(self) -> Tuple[Tuple[str, int], ...]:
        return tuple(sorted(self.document["key"]))

    @cached_property
    def options(self) -> Mapping[str, Any]:
        return {
            k: v for k, v in self.document.items() if k not in ("key", "v")
        }

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, IndexModel):
            return False
        return self.keys == other.keys and self.options == other.options

    @classmethod
    def iter_indexes(cls, index_info: Mapping[str, Any]) -> Iterator[Self]:
        for name, details in index_info.items():
            keys = details.pop("key")
            if ("_id", 1) in keys:
                continue
            yield cls(keys, name=name, **details)

    @classmethod
    def merge_indexes(cls, left: List[Self], right: List[Self]) -> List[Self]:
        merged = {index.keys: index for index in left}
        for index in right:
            merged[index.keys] = index
        return list(merged.values())

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> core_schema.CoreSchema:
        def validate(v: Any) -> IndexModel:
            if isinstance(v, cls):
                return v
            if isinstance(v, pymongo.IndexModel):
                return cls.from_pymongo(v)
            return cls(v)

        return core_schema.no_info_before_validator_function(
            validate, schema=handler(source_type)
        )


class IndexModelFactory:
    def __init__(self, index_type: int, **kwargs: Any):
        self.index_type = index_type
        self.kwargs = kwargs

    def __call__(self, key: str) -> IndexModel:
        return IndexModel([(key, self.index_type)], **self.kwargs)


def Indexed(
    annotation: Union[type, "GenericAlias"],
    index_type: int = pymongo.ASCENDING,
    **kwargs: Any,
) -> Any:
    """Returns an Annotated type with an `IndexModelFactory` as metadata."""
    return Annotated[annotation, IndexModelFactory(index_type, **kwargs)]
