from typing import Any, ClassVar, Mapping

from beanie.odm.operators import BaseFieldOperator, BaseOperator


class LogicalOperator(BaseOperator):
    operator: ClassVar[str]
    allow_scalar: ClassVar[bool] = True

    def __new__(cls, *expressions: Mapping[str, Any]) -> Any:
        if not expressions:
            raise AttributeError("At least one expression must be provided")
        if len(expressions) == 1 and cls.allow_scalar:
            expression = expressions[0]
            if isinstance(expression, BaseOperator) or len(expression) != 1:
                return expression
        return super().__new__(cls)

    def __init__(self, *expressions: Mapping[str, Any]):
        if len(expressions) == 1 and self.allow_scalar:
            expression = expressions[0]
            assert len(expression) == 1, expression
            key, value = next(iter(expression.items()))
        else:
            key = self.operator
            value = list(expressions)
        super().__init__(key, value)


class Or(LogicalOperator):
    """
    `$or` query operator

    Example:

    ```python
    class Product(Document):
        price: float
        category: str

    Or(Product.price<10, Product.category=="Sweets")
    ```

    Will return query object like

    ```python
    {"$or": [{"price": {"$lt": 10}}, {"category": "Sweets"}]}
    ```

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/or/>
    """

    operator = "$or"
    allow_scalar = True


class And(LogicalOperator):
    """
    `$and` query operator

    Example:

    ```python
    class Product(Document):
        price: float
        category: str

    And(Product.price<10, Product.category=="Sweets")
    ```

    Will return query object like

    ```python
    {"$and": [{"price": {"$lt": 10}}, {"category": "Sweets"}]}
    ```

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/and/>
    """

    operator = "$and"
    allow_scalar = True


class Nor(LogicalOperator):
    """
    `$nor` query operator

    Example:

    ```python
    class Product(Document):
        price: float
        category: str

    Nor(Product.price<10, Product.category=="Sweets")
    ```

    Will return query object like

    ```python
    {"$nor": [{"price": {"$lt": 10}}, {"category": "Sweets"}]}
    ```

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/nor/>
    """

    operator = "$nor"
    allow_scalar = False


class Not(BaseFieldOperator):
    """
    `$not` query operator

    Example:

    ```python
    class Product(Document):
        price: float
        category: str

    Not(Product.price<10)
    ```

    Will return query object like

    ```python
    {"$not": {"price": {"$lt": 10}}}
    ```

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/not/>
    """

    operator = "$not"

    def __init__(self, expression: Mapping[str, Any]):
        if len(expression) != 1:
            raise AttributeError(
                "Not operator can only be used with one expression"
            )

        key, value = next(iter(expression.items()))
        if key.startswith("$"):
            raise AttributeError("Not operator can not be used with operators")

        if not isinstance(value, Mapping):
            value = {"$eq": value}
        super().__init__(key, value)
