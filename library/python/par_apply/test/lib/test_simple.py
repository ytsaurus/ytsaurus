import pytest

import library.python.par_apply as pa


def test_simple_1():
    assert list(pa.par_apply([1, 2, 3], lambda x: x * x, 32)) == [1, 4, 9]


def test_simple_2():
    assert list(pa.par_apply([], lambda x: x / 0, 32)) == []


def test_simple_3():
    lst1 = []
    lst2 = []

    for i in range(0, 100000):
        lst1.append(i)
        lst2.append(i * i)

    assert list(pa.par_apply(lst1, lambda x: x * x, 32)) == lst2


def test_exc_1():
    def throw(x):
        raise Exception('1')

    with pytest.raises(Exception) as excinfo:
        list(pa.par_apply([1], throw, 32))

    assert str(excinfo.value) == '1'


def test_exc_traceback():
    def throw(x):
        raise Exception('1')

    with pytest.raises(Exception) as excinfo:
        list(pa.par_apply([1], throw, 32))

    assert excinfo.traceback[-1].frame.code.name == throw.__name__
