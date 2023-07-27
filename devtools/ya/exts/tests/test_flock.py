import exts.plocker as ep


class TestFlock(object):
    def test_import(self):
        lock = ep.Lock("lock", timeout=-1, flags=ep.LOCK_SH | ep.LOCK_NB)
