from beanie.odm.operators import BaseOperator


class Bit(BaseOperator):
    """
    `$bit` update query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/update/bit/>
    """

    def __init__(self, expression: dict):
        self.expression = expression

    @property
    def query(self):
        return {"$bit": self.expression}
