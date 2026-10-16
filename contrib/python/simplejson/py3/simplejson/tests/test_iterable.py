import unittest
from simplejson.compat import StringIO

import simplejson as json

def iter_dumps(obj, **kw):
    return ''.join(json.JSONEncoder(**kw).iterencode(obj))

def sio_dump(obj, **kw):
    sio = StringIO()
    json.dumps(obj, **kw)
    return sio.getvalue()

class BadIter:
    """Object whose __iter__ raises a non-TypeError exception."""
    def __init__(self, exc_type):
        self.exc_type = exc_type
    def __iter__(self):
        raise self.exc_type("from __iter__")


class TestIterable(unittest.TestCase):
    def test_iterable(self):
        for l in ([], [1], [1, 2], [1, 2, 3]):
            for opts in [{}, {'indent': 2}]:
                for dumps in (json.dumps, iter_dumps, sio_dump):
                    expect = dumps(l, **opts)
                    default_expect = dumps(sum(l), **opts)
                    # Default is False
                    self.assertRaises(TypeError, dumps, iter(l), **opts)
                    self.assertRaises(TypeError, dumps, iter(l), iterable_as_array=False, **opts)
                    self.assertEqual(expect, dumps(iter(l), iterable_as_array=True, **opts))
                    # Ensure that the "default" gets called
                    self.assertEqual(default_expect, dumps(iter(l), default=sum, **opts))
                    self.assertEqual(default_expect, dumps(iter(l), iterable_as_array=False, default=sum, **opts))
                    # Ensure that the "default" does not get called
                    self.assertEqual(
                        expect,
                        dumps(iter(l), iterable_as_array=True, default=sum, **opts))

    def test_iterable_as_array_propagates_non_typeerror(self):
        # Regression test: MemoryError from __iter__ must not be
        # swallowed by PyErr_Clear and replaced with TypeError.
        # Test against the C encoder directly since json.dumps may
        # use the Python fallback encoder.
        try:
            import simplejson._speedups as sp
            import decimal
        except ImportError:
            return  # C extension not available
        def noop_default(obj):
            raise TypeError('not serializable')
        c_enc = sp.make_encoder(
            {}, noop_default, sp.encode_basestring_ascii, None,
            ', ', ': ', False, False, True, {}, False, False,
            False, None, None, 'utf-8', False, False,
            decimal.Decimal, True,  # iterable_as_array=True
        )
        for exc_type in (MemoryError, RuntimeError):
            with self.assertRaises(exc_type):
                c_enc(BadIter(exc_type), 0)
