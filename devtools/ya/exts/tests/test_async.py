import six
import time
import pytest

import exts.asyncthread as ea


def test_future_1():
    def func():
        time.sleep(0.1)
        return 42

    af = ea.future(func)
    assert af() == 42
    assert af() == 42


def test_future_2():
    def func():
        time.sleep(0.1)
        return 42

    af = ea.future(func)

    def check():
        return af()

    fs = []
    for i in range(5):
        fs.append(ea.future(check))

    for f in fs:
        assert f() == 42

    for f in fs:
        assert f() == 42


class _SettableBool(object):
    def __init__(self):
        self.value = False

    def set(self):
        self.value = True


def test_cancellable_timer():
    ctx = _SettableBool()
    timer = ea.CancellableTimer(ctx.set, 2)
    timer.start()
    assert not ctx.value
    time.sleep(1)
    assert not ctx.value
    time.sleep(2)
    assert ctx.value


def test_cancellable_timer_cancel():
    ctx = _SettableBool()
    timer = ea.CancellableTimer(ctx.set, 2)
    timer.start()
    assert not ctx.value
    time.sleep(1)
    assert not ctx.value
    timer.cancel()
    assert not ctx.value
    time.sleep(2)
    assert not ctx.value


@pytest.mark.parametrize('threads', (1, 10))
def test_par_map(threads):
    result = ea.par_map(lambda x: x + 1, list(six.moves.range(10)), threads)
    assert result == list(range(1, 11))
