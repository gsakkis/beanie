from beanie.odm.operators import BaseOperator


class BaseFindComparisonOperator(BaseOperator):
    operator = ""

    def __init__(self, field, other):
        self.field = field
        self.other = other

    @property
    def query(self):
        return {self.field: {self.operator: self.other}}


class Eq(BaseFindComparisonOperator):
    """
    `$eq` query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/eq/>
    """

    @property
    def query(self):
        return {self.field: self.other}


class NE(BaseFindComparisonOperator):
    """
    `$ne` query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/ne/>
    """

    operator = "$ne"


class GT(BaseFindComparisonOperator):
    """
    `$gt` query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/gt/>
    """

    operator = "$gt"


class GTE(BaseFindComparisonOperator):
    """
    `$gte` query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/gte/>
    """

    operator = "$gte"


class LT(BaseFindComparisonOperator):
    """
    `$lt` query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/lt/>
    """

    operator = "$lt"


class LTE(BaseFindComparisonOperator):
    """
    `$lte` query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/lte/>
    """

    operator = "$lte"


class In(BaseFindComparisonOperator):
    """
    `$in` query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/in/>
    """

    operator = "$in"


class NotIn(BaseFindComparisonOperator):
    """
    `$nin` query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/nin/>
    """

    operator = "$nin"
