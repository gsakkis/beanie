import pytest

from beanie.odm.operators.logical import And, Nor, Not, Or
from tests.odm.models import Sample


async def test_and():
    with pytest.raises(AttributeError):
        And()

    q = And(Sample.integer == 1)
    assert q == {"integer": 1}

    q = And({"integer": 1})
    assert q == {"integer": 1}

    q = And({})
    assert q == {}

    q = And(Sample.integer == 1, Sample.nested.integer > 3)
    assert q == {"$and": [{"integer": 1}, {"nested.integer": {"$gt": 3}}]}


async def test_not():
    q = Not(Sample.integer == 1)
    assert q == {"$not": {"integer": 1}}

    q = Not({"integer": 1})
    assert q == {"$not": {"integer": 1}}


async def test_nor():
    with pytest.raises(AttributeError):
        Nor()

    q = Nor(Sample.integer == 1)
    assert q == {"$nor": [{"integer": 1}]}

    q = Nor({"integer": 1})
    assert q == {"$nor": [{"integer": 1}]}

    q = Nor({})
    assert q == {"$nor": [{}]}

    q = Nor(Sample.integer == 1, Sample.nested.integer > 3)
    assert q == {"$nor": [{"integer": 1}, {"nested.integer": {"$gt": 3}}]}


async def test_or():
    with pytest.raises(AttributeError):
        Or()

    q = Or(Sample.integer == 1)
    assert q == {"integer": 1}

    q = Or({"integer": 1})
    assert q == {"integer": 1}

    q = Or({})
    assert q == {}

    q = Or(Sample.integer == 1, Sample.nested.integer > 3)
    assert q == {"$or": [{"integer": 1}, {"nested.integer": {"$gt": 3}}]}
