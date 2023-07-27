import library.python.codecs as c


def test_simple():
    cmpr = c.dumps('zstd_5', b'xxx')

    assert cmpr != b'xxx'
    assert c.loads('zstd_5', cmpr) == b'xxx'


def test_list():
    assert len(c.list_all_codecs()) > 10


def test_all():
    for name in c.list_all_codecs():
        assert c.loads(name, c.dumps(name, b'xxx')) == b'xxx'
