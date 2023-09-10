from beanie.odm.operators import BaseOperator


class BaseFindBitwiseOperator(BaseOperator):
    operator = ""

    def __init__(self, field, bitmask):
        self.field = field
        self.bitmask = bitmask

    @property
    def query(self):
        return {self.field: {self.operator: self.bitmask}}


class BitsAllClear(BaseFindBitwiseOperator):
    """
    `$bitsAllClear` query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/bitsAllClear/>
    """

    operator = "$bitsAllClear"


class BitsAllSet(BaseFindBitwiseOperator):
    """
    `$bitsAllSet` query operator

    MongoDB doc:
    https://docs.mongodb.com/manual/reference/operator/query/bitsAllSet/
    """

    operator = "$bitsAllSet"


class BitsAnyClear(BaseFindBitwiseOperator):
    """
    `$bitsAnyClear` query operator

    MongoDB doc:
    https://docs.mongodb.com/manual/reference/operator/query/bitsAnyClear/
    """

    operator = "$bitsAnyClear"


class BitsAnySet(BaseFindBitwiseOperator):
    """
    `$bitsAnySet` query operator

    MongoDB doc:
    https://docs.mongodb.com/manual/reference/operator/query/bitsAnySet/
    """

    operator = "$bitsAnySet"
