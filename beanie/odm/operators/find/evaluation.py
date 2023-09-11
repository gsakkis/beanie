from typing import Any, Dict, Optional

from beanie.odm.operators import BaseOperator


class Expr(BaseOperator):
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

    def __init__(self, expression: dict):
        self.expression = expression

    @property
    def query(self):
        return {self.operator: self.expression}


class JsonSchema(BaseOperator):
    """
    `$jsonSchema` query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/jsonSchema/>
    """

    operator = "$jsonSchema"

    def __init__(self, expression: dict):
        self.expression = expression

    @property
    def query(self):
        return {self.operator: self.expression}


class Mod(BaseOperator):
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

    def __init__(self, field, divisor: int, remainder: int):
        self.field = field
        self.expression = [divisor, remainder]

    @property
    def query(self):
        return {self.field: {self.operator: self.expression}}


class RegEx(BaseOperator):
    """
    `$regex` query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/regex/>
    """

    operator = "$regex"

    def __init__(self, field, pattern: str, options: Optional[str] = None):
        self.field = field
        self.expression = {self.operator: pattern}
        if options:
            self.expression["$options"] = options

    @property
    def query(self):
        return {self.field: self.expression}


class Text(BaseOperator):
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
        self.expression: Dict[str, Any] = {"$search": search}
        if language is not None:
            self.expression["$language"] = language
        if case_sensitive is not None:
            self.expression["$caseSensitive"] = case_sensitive
        if diacritic_sensitive is not None:
            self.expression["$diacriticSensitive"] = diacritic_sensitive

    @property
    def query(self):
        return {self.operator: self.expression}


class Where(BaseOperator):
    """
    `$where` query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/where/>
    """

    operator = "$where"

    def __init__(self, expression: str):
        self.expression = expression

    @property
    def query(self):
        return {self.operator: self.expression}
