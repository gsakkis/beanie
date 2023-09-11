from beanie.odm.operators import BaseNonFieldOperator


class Set(BaseNonFieldOperator):
    """
    `$set` update query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/update/set/>
    """

    operator = "$set"


class SetRevisionId(Set):
    def __init__(self, revision_id):
        super().__init__({"revision_id": revision_id})


class CurrentDate(BaseNonFieldOperator):
    """
    `$currentDate` update query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/update/currentDate/>
    """

    operator = "$currentDate"


class Inc(BaseNonFieldOperator):
    """
    `$inc` update query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/update/inc/>
    """

    operator = "$inc"


class Min(BaseNonFieldOperator):
    """
    `$min` update query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/update/min/>
    """

    operator = "$min"


class Max(BaseNonFieldOperator):
    """
    `$max` update query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/update/max/>
    """

    operator = "$max"


class Mul(BaseNonFieldOperator):
    """
    `$mul` update query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/update/mul/>
    """

    operator = "$mul"


class Rename(BaseNonFieldOperator):
    """
    `$rename` update query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/update/rename/>
    """

    operator = "$rename"


class SetOnInsert(BaseNonFieldOperator):
    """
    `$setOnInsert` update query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/update/setOnInsert/>
    """

    operator = "$setOnInsert"


class Unset(BaseNonFieldOperator):
    """
    `$unset` update query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/update/unset/>
    """

    operator = "$unset"


class Bit(BaseNonFieldOperator):
    """
    `$bit` update query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/update/bit/>
    """

    operator = "$bit"


class AddToSet(BaseNonFieldOperator):
    """
    `$addToSet` update array query operator

    MongoDB docs:
    <https://docs.mongodb.com/manual/reference/operator/update/addToSet/>
    """

    operator = "$addToSet"


class Pop(BaseNonFieldOperator):
    """
    `$pop` update array query operator

    MongoDB docs:
    <https://docs.mongodb.com/manual/reference/operator/update/pop/>
    """

    operator = "$pop"


class Pull(BaseNonFieldOperator):
    """
    `$pull` update array query operator

    MongoDB docs:
    <https://docs.mongodb.com/manual/reference/operator/update/pull/>
    """

    operator = "$pull"


class Push(BaseNonFieldOperator):
    """
    `$push` update array query operator

    MongoDB docs:
    <https://docs.mongodb.com/manual/reference/operator/update/push/>
    """

    operator = "$push"


class PullAll(BaseNonFieldOperator):
    """
    `$pullAll` update array query operator

    MongoDB docs:
    <https://docs.mongodb.com/manual/reference/operator/update/pullAll/>
    """

    operator = "$pullAll"
