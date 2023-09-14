from typing import TYPE_CHECKING

from beanie.odm.operators import BaseFieldOperator

if TYPE_CHECKING:
    from beanie.odm.fields import FieldExpr


class Exists(BaseFieldOperator):
    """
    `$exists` query operator

    Example:

    ```python
    class Product(Document):
        price: float

    Exists(Product.price, True)
    ```

    Will return query object like

    ```python
    {"price": {"$exists": True}}
    ```

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/exists/>
    """

    operator = "$exists"

    def __init__(self, field: "FieldExpr", value: bool = True):
        super().__init__(field, value)


class Type(BaseFieldOperator):
    """
    `$type` query operator

    Example:

    ```python
    class Product(Document):
        price: float

    Type(Product.price, "decimal")
    ```

    Will return query object like

    ```python
    {"price": {"$type": "decimal"}}
    ```

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/type/>
    """

    operator = "$type"

    def __init__(self, field: "FieldExpr", *types: str):
        super().__init__(field, list(types) if len(types) > 1 else types[0])
