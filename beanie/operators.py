from beanie.odm.operators.array import All, ElemMatch, Size
from beanie.odm.operators.bitwise import (
    BitsAllClear,
    BitsAllSet,
    BitsAnyClear,
    BitsAnySet,
)
from beanie.odm.operators.comparison import (
    GT,
    GTE,
    LT,
    LTE,
    NE,
    Eq,
    In,
    NotIn,
)
from beanie.odm.operators.element import Exists, Type
from beanie.odm.operators.evaluation import (
    Expr,
    JsonSchema,
    Mod,
    RegEx,
    Text,
    Where,
)
from beanie.odm.operators.geospatial import (
    Box,
    GeoIntersects,
    GeoWithin,
    GeoWithinTypes,
    Near,
    NearSphere,
)
from beanie.odm.operators.logical import And, Nor, Not, Or
from beanie.odm.operators.update import (
    AddToSet,
    Bit,
    CurrentDate,
    Inc,
    Max,
    Min,
    Mul,
    Pop,
    Pull,
    PullAll,
    Push,
    Rename,
    Set,
    SetOnInsert,
    Unset,
)

__all__ = [
    # Find
    # Array
    "All",
    "ElemMatch",
    "Size",
    # Bitwise
    "BitsAllClear",
    "BitsAllSet",
    "BitsAnyClear",
    "BitsAnySet",
    # Comparison
    "Eq",
    "GT",
    "GTE",
    "In",
    "NotIn",
    "LT",
    "LTE",
    "NE",
    # Element
    "Exists",
    "Type",
    "Type",
    # Evaluation
    "Expr",
    "JsonSchema",
    "Mod",
    "RegEx",
    "Text",
    "Where",
    # Geospatial
    "GeoIntersects",
    "GeoWithinTypes",
    "GeoWithin",
    "Box",
    "Near",
    "NearSphere",
    # Logical
    "Or",
    "And",
    "Nor",
    "Not",
    # Update
    # Array
    "AddToSet",
    "Pop",
    "Pull",
    "Push",
    "PullAll",
    # Bitwise
    "Bit",
    # General
    "Set",
    "CurrentDate",
    "Inc",
    "Min",
    "Max",
    "Mul",
    "Rename",
    "SetOnInsert",
    "Unset",
]
