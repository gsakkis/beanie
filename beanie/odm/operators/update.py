from beanie.odm.operators import BaseOperator


class BaseUpdateOperator(BaseOperator):
    operator = ""

    def __init__(self, expression):
        self.expression = expression

    @property
    def query(self):
        return {self.operator: self.expression}


class Set(BaseUpdateOperator):
    """
    `$set` update query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/update/set/>
    """

    operator = "$set"


class SetRevisionId(Set):
    def __init__(self, revision_id):
        super().__init__({"revision_id": revision_id})


class CurrentDate(BaseUpdateOperator):
    """
    `$currentDate` update query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/update/currentDate/>
    """

    operator = "$currentDate"


class Inc(BaseUpdateOperator):
    """
    `$inc` update query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/update/inc/>
    """

    operator = "$inc"


class Min(BaseUpdateOperator):
    """
    `$min` update query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/update/min/>
    """

    operator = "$min"


class Max(BaseUpdateOperator):
    """
    `$max` update query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/update/max/>
    """

    operator = "$max"


class Mul(BaseUpdateOperator):
    """
    `$mul` update query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/update/mul/>
    """

    operator = "$mul"


class Rename(BaseUpdateOperator):
    """
    `$rename` update query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/update/rename/>
    """

    operator = "$rename"


class SetOnInsert(BaseUpdateOperator):
    """
    `$setOnInsert` update query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/update/setOnInsert/>
    """

    operator = "$setOnInsert"


class Unset(BaseUpdateOperator):
    """
    `$unset` update query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/update/unset/>
    """

    operator = "$unset"


class Bit(BaseUpdateOperator):
    """
    `$bit` update query operator

    MongoDB doc:
    <https://docs.mongodb.com/manual/reference/operator/update/bit/>
    """

    operator = "$bit"


class AddToSet(BaseUpdateOperator):
    """
    `$addToSet` update array query operator

    MongoDB docs:
    <https://docs.mongodb.com/manual/reference/operator/update/addToSet/>
    """

    operator = "$addToSet"


class Pop(BaseUpdateOperator):
    """
    `$pop` update array query operator

    MongoDB docs:
    <https://docs.mongodb.com/manual/reference/operator/update/pop/>
    """

    operator = "$pop"


class Pull(BaseUpdateOperator):
    """
    `$pull` update array query operator

    MongoDB docs:
    <https://docs.mongodb.com/manual/reference/operator/update/pull/>
    """

    operator = "$pull"


class Push(BaseUpdateOperator):
    """
    `$push` update array query operator

    MongoDB docs:
    <https://docs.mongodb.com/manual/reference/operator/update/push/>
    """

    operator = "$push"


class PullAll(BaseUpdateOperator):
    """
    `$pullAll` update array query operator

    MongoDB docs:
    <https://docs.mongodb.com/manual/reference/operator/update/pullAll/>
    """

    operator = "$pullAll"
