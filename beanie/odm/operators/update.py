from beanie.odm.operators import BaseOperator


class Set(BaseOperator):
    """
    `$set` update query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/update/set/>
    """

    operator = "$set"


class SetRevisionId(Set):
    def __init__(self, revision_id):
        super().__init__({"revision_id": revision_id})


class CurrentDate(BaseOperator):
    """
    `$currentDate` update query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/update/currentDate/>
    """

    operator = "$currentDate"


class Inc(BaseOperator):
    """
    `$inc` update query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/update/inc/>
    """

    operator = "$inc"


class Min(BaseOperator):
    """
    `$min` update query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/update/min/>
    """

    operator = "$min"


class Max(BaseOperator):
    """
    `$max` update query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/update/max/>
    """

    operator = "$max"


class Mul(BaseOperator):
    """
    `$mul` update query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/update/mul/>
    """

    operator = "$mul"


class Rename(BaseOperator):
    """
    `$rename` update query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/update/rename/>
    """

    operator = "$rename"


class SetOnInsert(BaseOperator):
    """
    `$setOnInsert` update query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/update/setOnInsert/>
    """

    operator = "$setOnInsert"


class Unset(BaseOperator):
    """
    `$unset` update query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/update/unset/>
    """

    operator = "$unset"


class Bit(BaseOperator):
    """
    `$bit` update query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/update/bit/>
    """

    operator = "$bit"


class AddToSet(BaseOperator):
    """
    `$addToSet` update array query operator

    MongoDB docs:
    <https://docs.mongodb.com/manual/reference/operator/update/addToSet/>
    """

    operator = "$addToSet"


class Pop(BaseOperator):
    """
    `$pop` update array query operator

    MongoDB docs:
    <https://docs.mongodb.com/manual/reference/operator/update/pop/>
    """

    operator = "$pop"


class Pull(BaseOperator):
    """
    `$pull` update array query operator

    MongoDB docs:
    <https://docs.mongodb.com/manual/reference/operator/update/pull/>
    """

    operator = "$pull"


class Push(BaseOperator):
    """
    `$push` update array query operator

    MongoDB docs:
    <https://docs.mongodb.com/manual/reference/operator/update/push/>
    """

    operator = "$push"


class PullAll(BaseOperator):
    """
    `$pullAll` update array query operator

    MongoDB docs:
    <https://docs.mongodb.com/manual/reference/operator/update/pullAll/>
    """

    operator = "$pullAll"
