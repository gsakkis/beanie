from typing import Any


class BaseOperator(dict):
    """Base operator"""

    operator: str

    def __init__(self, expression: Any):
        self[self.operator] = expression


class BaseFieldOperator(BaseOperator):
    def __init__(self, field: str, expression: Any):
        self[field] = {self.operator: expression}
