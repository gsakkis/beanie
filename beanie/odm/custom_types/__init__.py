import decimal
from typing import Any

import bson
import pydantic
from typing_extensions import Annotated


def _to_bson_binary(value: Any) -> bson.Binary:
    return value if isinstance(value, bson.Binary) else bson.Binary(value)


BsonBinary = Annotated[bson.Binary, pydantic.PlainValidator(_to_bson_binary)]

DecimalAnnotation = Annotated[
    decimal.Decimal,
    pydantic.BeforeValidator(
        lambda v: v.to_decimal() if isinstance(v, bson.Decimal128) else v
    ),
]
