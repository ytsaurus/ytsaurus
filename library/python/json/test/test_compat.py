from __future__ import print_function

import json
import sys
import tarfile

import six

import library.python.json as lpj
import library.python.resource as lpr


def canon(v):
    return json.dumps(v, indent=4, sort_keys=True)


def run(v):
    d = canon(v)

    assert d == canon(lpj.loads(d))
    assert d == canon(lpj.loads(six.text_type(d)))


def run_tgz(name):
    io = six.BytesIO(lpr.find('/' + name))
    tf = tarfile.open(fileobj=io, mode='r:gz')

    for x in tf:
        if x.isreg():
            data = tf.extractfile(x).read()

            try:
                json.loads(data)
            except:
                continue

            try:
                assert canon(lpj.loads(data)) == canon(json.loads(data))
            except Exception as e:
                if '/i_' in x.name or '/n_' in x.name:
                    print("Suppressed error in " + x.name + ":", file=sys.stderr)
                    print(e, file=sys.stderr)
                else:
                    raise


def test_milo():
    run_tgz('data.tar.gz')


def test_JSONTestSuite():
    run_tgz('test_parsing.tar.gz')


def test_compat():
    for s in (-1, -2, -1000000, -(2**32), -(2**32 - 1), -(2**31), -(2**31 - 1)):
        run(s)

    for s in (0, 1, -1, '', 'st', 'qwтестqw', 0.5, -12.1, [], {}, [1], {'1': 2}, [{}], [1, {}], [5, 6, {}, {'2': {}}], {'1': []}, {'2': [3], '4': 5}):
        run(s)


def test_unicode():
    run(u'тест')


def test_unicode_2():
    a = u'"фыв"'

    lpj.loads(a)


def test_surrogate_pairs():
    data = u'"≡ \\u0007[ДТП\\u0007] видео. \\u0007[ДТП\\u0007] легковых \\uD83D\\uDE97 Грузовики: \\u0007[ДТП\\u0007], приколы Мото \\u0007[ДТП\\u0007] и приколы."'

    if six.PY3:
        # Under python3 string literals are returned as native utf8 unicode strings...
        assert lpj.loads(data.encode('utf-8')) == json.loads(data.encode('utf-8'))
    else:
        # ...while under python2 strings are utf-8 encoded
        assert lpj.loads(data.encode('utf-8')) == json.loads(data.encode('utf-8')).encode('utf-8')


def test_exc():
    err = ''

    try:
        lpj.loads('{]')
    except Exception as e:
        err = str(e)

    assert err


def test_1():
    x = int(72075186224037918)

    x_json = json.loads(json.dumps(x))
    x_lpj = lpj.loads(lpj.dumps(x))

    assert x_json == x_lpj
    assert type(x_json) == type(x_lpj)


def test_big_int():
    lpj.loads(json.dumps({'value': 1111111111111111111111111111111111111111111111111111111111111111111111111111}))
