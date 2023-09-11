from beanie.odm.operators import BaseOperator


class Exists(BaseOperator):
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

    def __init__(self, field, value: bool = True):
        self.field = field
        self.value = value

    @property
    def query(self):
        return {self.field: {self.operator: self.value}}


class Type(BaseOperator):
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

    def __init__(self, field, *types: str):
        self.field = field
        self.types = list(types) if len(types) > 1 else types[0]

    @property
    def query(self):
        return {self.field: {self.operator: self.types}}
