from typing import TYPE_CHECKING, Any, Mapping, Optional

from beanie.odm.operators import BaseFieldOperator

if TYPE_CHECKING:
    from beanie.odm.fields import FieldExpr


class All(BaseFieldOperator):
    """
    `$all` array query operator

    Example:

    ```python
    class Sample(Document):
        results: List[int]

    All(Sample.results, [80, 85])
    ```

    Will return query object like

    ```python
    {"results": {"$all": [80, 85]}}
    ```

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/all>
    """

    operator = "$all"


class ElemMatch(BaseFieldOperator):
    """
    `$elemMatch` array query operator

    Example:

    ```python
    class Sample(Document):
        results: List[int]

    ElemMatch(Sample.results, {"$in": [80, 85]})
    ```

    Will return query object like

    ```python
    {"results": {"$elemMatch": {"$in": [80, 85]}}}
    ```

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/elemMatch/>
    """

    operator = "$elemMatch"

    def __init__(
        self,
        field: "FieldExpr",
        expression: Optional[Mapping[str, Any]] = None,
        **kwargs: Any,
    ):
        super().__init__(
            field,
            dict(expression, **kwargs) if expression is not None else kwargs,
        )


class Size(BaseFieldOperator):
    """
    `$size` array query operator

    Example:

    ```python
    class Sample(Document):
        results: List[int]

    Size(Sample.results, 2)
    ```

    Will return query object like

    ```python
    {"results": {"$size": 2}}
    ```

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/size/>
    """

    operator = "$size"
