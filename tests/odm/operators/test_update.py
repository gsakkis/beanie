from beanie.odm.operators.update import (
    AddToSet,
    Bit,
    CurrentDate,
    Inc,
    Max,
    Min,
    Mul,
    Pop,
    Pull,
    PullAll,
    Push,
    Rename,
    Set,
    SetOnInsert,
    Unset,
)
from tests.odm.models import Sample


def test_set():
    q = Set({Sample.integer: 2})
    assert q == {"$set": {"integer": 2}}


def test_current_date():
    q = CurrentDate({Sample.integer: 2})
    assert q == {"$currentDate": {"integer": 2}}


def test_inc():
    q = Inc({Sample.integer: 2})
    assert q == {"$inc": {"integer": 2}}


def test_min():
    q = Min({Sample.integer: 2})
    assert q == {"$min": {"integer": 2}}


def test_max():
    q = Max({Sample.integer: 2})
    assert q == {"$max": {"integer": 2}}


def test_mul():
    q = Mul({Sample.integer: 2})
    assert q == {"$mul": {"integer": 2}}


def test_rename():
    q = Rename({Sample.integer: 2})
    assert q == {"$rename": {"integer": 2}}


def test_set_on_insert():
    q = SetOnInsert({Sample.integer: 2})
    assert q == {"$setOnInsert": {"integer": 2}}


def test_unset():
    q = Unset({Sample.integer: 2})
    assert q == {"$unset": {"integer": 2}}


def test_add_to_set():
    q = AddToSet({Sample.integer: 2})
    assert q == {"$addToSet": {"integer": 2}}


def test_pop():
    q = Pop({Sample.integer: 2})
    assert q == {"$pop": {"integer": 2}}


def test_pull():
    q = Pull({Sample.integer: 2})
    assert q == {"$pull": {"integer": 2}}


def test_push():
    q = Push({Sample.integer: 2})
    assert q == {"$push": {"integer": 2}}


def test_pull_all():
    q = PullAll({Sample.integer: 2})
    assert q == {"$pullAll": {"integer": 2}}


def test_bit():
    q = Bit({Sample.integer: 2})
    assert q == {"$bit": {"integer": 2}}
