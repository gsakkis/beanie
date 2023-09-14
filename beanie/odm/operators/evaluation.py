from typing import TYPE_CHECKING, Any, Dict, Optional

from beanie.odm.operators import BaseFieldOperator, BaseNonFieldOperator

if TYPE_CHECKING:
    from beanie.odm.fields import FieldExpr


class Expr(BaseNonFieldOperator):
    """
    `$expr` query operator

    Example:

    ```python
    class Sample(Document):
        one: int
        two: int

    Expr({"$gt": [ "$one" , "$two" ]})
    ```

    Will return query object like

    ```python
    {"$expr": {"$gt": [ "$one" , "$two" ]}}
    ```

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/expr/>
    """

    operator = "$expr"


class JsonSchema(BaseNonFieldOperator):
    """
    `$jsonSchema` query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/jsonSchema/>
    """

    operator = "$jsonSchema"


class Mod(BaseFieldOperator):
    """
    `$mod` query operator

    Example:

    ```python
    class Sample(Document):
        one: int

    Mod(Sample.one, 4, 0)
    ```

    Will return query object like

    ```python
    { "one": { "$mod": [ 4, 0 ] } }
    ```

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/mod/>
    """

    operator = "$mod"

    def __init__(self, field: "FieldExpr", divisor: int, remainder: int):
        super().__init__(field, [divisor, remainder])


class RegEx(BaseFieldOperator):
    """
    `$regex` query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/regex/>
    """

    operator = "$regex"

    def __init__(
        self, field: "FieldExpr", pattern: str, options: Optional[str] = None
    ):
        super().__init__(field, pattern)
        if options:
            self._value["$options"] = options


class Text(BaseNonFieldOperator):
    """
    `$text` query operator

    Example:

    ```python
    class Sample(Document):
        description: Indexed(str, pymongo.TEXT)

    Text("coffee")
    ```

    Will return query object like

    ```python
    {
        "$text": {
            "$search": "coffee" ,
            "$caseSensitive": False,
            "$diacriticSensitive": False
        }
    }
    ```

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/text/>
    """

    operator = "$text"

    def __init__(
        self,
        search: str,
        language: Optional[str] = None,
        case_sensitive: Optional[bool] = None,
        diacritic_sensitive: Optional[bool] = None,
    ):
        expression: Dict[str, Any] = {"$search": search}
        if language is not None:
            expression["$language"] = language
        if case_sensitive is not None:
            expression["$caseSensitive"] = case_sensitive
        if diacritic_sensitive is not None:
            expression["$diacriticSensitive"] = diacritic_sensitive
        super().__init__(expression)


class Where(BaseNonFieldOperator):
    """
    `$where` query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/where/>
    """

    operator = "$where"
