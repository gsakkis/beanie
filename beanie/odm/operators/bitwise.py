from beanie.odm.operators import BaseFieldOperator


class BitsAllClear(BaseFieldOperator):
    """
    `$bitsAllClear` query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/query/bitsAllClear/>
    """

    operator = "$bitsAllClear"


class BitsAllSet(BaseFieldOperator):
    """
    `$bitsAllSet` query operator

    MongoDB doc:
    https://docs.mongodb.com/manual/reference/operator/query/bitsAllSet/
    """

    operator = "$bitsAllSet"


class BitsAnyClear(BaseFieldOperator):
    """
    `$bitsAnyClear` query operator

    MongoDB doc:
    https://docs.mongodb.com/manual/reference/operator/query/bitsAnyClear/
    """

    operator = "$bitsAnyClear"


class BitsAnySet(BaseFieldOperator):
    """
    `$bitsAnySet` query operator

    MongoDB doc:
    https://docs.mongodb.com/manual/reference/operator/query/bitsAnySet/
    """

    operator = "$bitsAnySet"
