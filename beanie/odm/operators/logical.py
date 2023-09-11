from beanie.odm.operators import BaseOperator


class LogicalOperatorForListOfExpressions(BaseOperator):
    operator = ""
    allow_scalar = True

    def __init__(self, *expressions):
        if not expressions:
            raise AttributeError("At least one expression must be provided")

        if self.allow_scalar and len(expressions) == 1:
            self.update(expressions[0])
        else:
            super().__init__(list(expressions))


class Or(LogicalOperatorForListOfExpressions):
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


class And(LogicalOperatorForListOfExpressions):
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


class Nor(LogicalOperatorForListOfExpressions):
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


class Not(BaseOperator):
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
