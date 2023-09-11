from beanie.odm.operators import BaseFieldOperator


class Eq(BaseFieldOperator):
    """
    `$eq` query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/eq/>
    """

    operator = "$eq"

    def __init__(self, field, expression):
        super().__init__(field, expression)
        self[field] = expression


class NE(BaseFieldOperator):
    """
    `$ne` query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/ne/>
    """

    operator = "$ne"


class GT(BaseFieldOperator):
    """
    `$gt` query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/gt/>
    """

    operator = "$gt"


class GTE(BaseFieldOperator):
    """
    `$gte` query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/gte/>
    """

    operator = "$gte"


class LT(BaseFieldOperator):
    """
    `$lt` query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/lt/>
    """

    operator = "$lt"


class LTE(BaseFieldOperator):
    """
    `$lte` query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/lte/>
    """

    operator = "$lte"


class In(BaseFieldOperator):
    """
    `$in` query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/in/>
    """

    operator = "$in"


class NotIn(BaseFieldOperator):
    """
    `$nin` query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/nin/>
    """

    operator = "$nin"
