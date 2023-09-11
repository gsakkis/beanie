class BaseOperator(dict):
    """Base operator"""

    operator = ""


class BaseNonFieldOperator(BaseOperator):
    def __init__(self, expression):
        super().__init__()
        self[self.operator] = expression


class BaseFieldOperator(BaseOperator):
    def __init__(self, field, expression):
        super().__init__()
        self[field] = {self.operator: expression}
