class BaseOperator(dict):
    """Base operator"""

    operator: str

    def __init__(self, expression):
        self[self.operator] = expression


class BaseFieldOperator(BaseOperator):
    def __init__(self, field, expression):
        self[field] = {self.operator: expression}
