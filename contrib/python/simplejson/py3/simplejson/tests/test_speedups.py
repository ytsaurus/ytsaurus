import sys
import unittest
from unittest import TestCase

import simplejson
from simplejson import encoder, decoder, scanner
from simplejson.compat import PY3, long_type, b
from ._helpers import has_speedups, skip_if_speedups_missing


class BadBool:
    def __bool__(self):
        1/0
    __nonzero__ = __bool__


class TestDecode(TestCase):
    @skip_if_speedups_missing
    def test_make_scanner(self):
        self.assertRaises(AttributeError, scanner.c_make_scanner, 1)

    @skip_if_speedups_missing
    def test_bad_bool_args(self):
        def test(value):
            decoder.JSONDecoder(strict=BadBool()).decode(value)
        self.assertRaises(ZeroDivisionError, test, '""')
        self.assertRaises(ZeroDivisionError, test, '{}')
        if not PY3:
            self.assertRaises(ZeroDivisionError, test, u'""')
            self.assertRaises(ZeroDivisionError, test, u'{}')

class TestEncode(TestCase):
    @skip_if_speedups_missing
    def test_make_encoder(self):
        self.assertRaises(
            TypeError,
            encoder.c_make_encoder,
            None,
            ("\xCD\x7D\x3D\x4E\x12\x4C\xF9\x79\xD7"
             "\x52\xBA\x82\xF2\x27\x4A\x7D\xA0\xCA\x75"),
            None
        )

    @skip_if_speedups_missing
    def test_bad_str_encoder(self):
        # Issue #31505: There shouldn't be an assertion failure in case
        # c_make_encoder() receives a bad encoder() argument.
        import decimal
        def bad_encoder1(*args):
            return None
        enc = encoder.c_make_encoder(
                None, lambda obj: str(obj),
                bad_encoder1, None, ': ', ', ',
                False, False, False, {}, False, False, False,
                None, None, 'utf-8', False, False, decimal.Decimal, False)
        self.assertRaises(TypeError, enc, 'spam', 4)
        self.assertRaises(TypeError, enc, {'spam': 42}, 4)

        def bad_encoder2(*args):
            1/0
        enc = encoder.c_make_encoder(
                None, lambda obj: str(obj),
                bad_encoder2, None, ': ', ', ',
                False, False, False, {}, False, False, False,
                None, None, 'utf-8', False, False, decimal.Decimal, False)
        self.assertRaises(ZeroDivisionError, enc, 'spam', 4)

    @skip_if_speedups_missing
    def test_bad_bool_args(self):
        def test(name):
            encoder.JSONEncoder(**{name: BadBool()}).encode({})
        self.assertRaises(ZeroDivisionError, test, 'skipkeys')
        self.assertRaises(ZeroDivisionError, test, 'ensure_ascii')
        self.assertRaises(ZeroDivisionError, test, 'check_circular')
        self.assertRaises(ZeroDivisionError, test, 'allow_nan')
        self.assertRaises(ZeroDivisionError, test, 'sort_keys')
        self.assertRaises(ZeroDivisionError, test, 'use_decimal')
        self.assertRaises(ZeroDivisionError, test, 'namedtuple_as_object')
        self.assertRaises(ZeroDivisionError, test, 'tuple_as_array')
        self.assertRaises(ZeroDivisionError, test, 'bigint_as_string')
        self.assertRaises(ZeroDivisionError, test, 'for_json')
        self.assertRaises(ZeroDivisionError, test, 'ignore_nan')
        self.assertRaises(ZeroDivisionError, test, 'iterable_as_array')

    @skip_if_speedups_missing
    def test_int_as_string_bitcount_overflow(self):
        long_count = long_type(2)**32+31
        def test():
            encoder.JSONEncoder(int_as_string_bitcount=long_count).encode(0)
        self.assertRaises((TypeError, OverflowError), test)

    if PY3:
        @skip_if_speedups_missing
        def test_bad_encoding(self):
            with self.assertRaises(UnicodeEncodeError):
                encoder.JSONEncoder(encoding='\udcff').encode({b('key'): 123})


@unittest.skipIf(sys.version_info < (3, 13),
                 "heap types require Python 3.13+")
class TestHeapTypes(TestCase):
    """Verify that Scanner and Encoder are heap types on Python 3.13+."""

    @skip_if_speedups_missing
    def test_scanner_is_heap_type(self):
        from simplejson._speedups import make_scanner
        # Py_TPFLAGS_HEAPTYPE = 1 << 9
        self.assertTrue(make_scanner.__flags__ & (1 << 9),
                        "Scanner should be a heap type on 3.13+")

    @skip_if_speedups_missing
    def test_encoder_is_heap_type(self):
        from simplejson._speedups import make_encoder
        self.assertTrue(make_encoder.__flags__ & (1 << 9),
                        "Encoder should be a heap type on 3.13+")

    @skip_if_speedups_missing
    def test_scanner_type_is_gc_tracked(self):
        """Heap types must be GC-tracked so they can be collected."""
        import gc
        from simplejson._speedups import make_scanner
        self.assertTrue(gc.is_tracked(make_scanner))

    @skip_if_speedups_missing
    def test_encoder_type_is_gc_tracked(self):
        import gc
        from simplejson._speedups import make_encoder
        self.assertTrue(gc.is_tracked(make_encoder))

    @skip_if_speedups_missing
    def test_scanner_instances_work(self):
        """Verify Scanner heap type instances decode correctly."""
        result = simplejson.loads('{"a": 1}')
        self.assertEqual(result, {"a": 1})

    @skip_if_speedups_missing
    def test_encoder_instances_work(self):
        """Verify Encoder heap type instances encode correctly."""
        result = simplejson.dumps({"a": 1}, sort_keys=True)
        self.assertEqual(result, '{"a": 1}')


@unittest.skipUnless(hasattr(sys, "gettotalrefcount"),
                     "debug build required (sys.gettotalrefcount)")
class TestRefcountLeaks(TestCase):
    """Catch refcount leaks in the C extension.

    These tests only run on debug builds of CPython, which expose
    sys.gettotalrefcount(). On release builds they skip silently.
    """

    ITER = 2000
    WARMUP = 200

    def _assert_no_leak(self, func):
        """Run `func` in two measurement phases and verify the second
        phase's refcount delta stays near zero.

        A real per-call leak (1 ref per call) grows linearly with the
        iteration count, so both phase1 and phase2 would be ~ITER. But
        front-loaded noise -- specializer inline caches, dict resize,
        gc generation bumps, etc. -- shows up entirely in phase1 and
        leaves phase2 near zero. Asserting on phase2 only is thus both
        more sensitive (catches smaller linear leaks) and more robust
        (no false positives from CPython internals).
        """
        import gc
        # Stabilize caches, specializer, intern pools, etc.
        for _ in range(self.WARMUP):
            func()
            gc.collect()

        # Collect every iteration so cyclic garbage doesn't accumulate
        # across GC generations and cause noisy refcount deltas.
        start = sys.gettotalrefcount()
        for _ in range(self.ITER):
            func()
            gc.collect()
        mid = sys.gettotalrefcount()
        for _ in range(self.ITER):
            func()
            gc.collect()
        end = sys.gettotalrefcount()

        phase1 = mid - start
        phase2 = end - mid
        msg = ("phase1=%d, phase2=%d, iterations=%d. A real per-call "
               "leak would make phase2 grow linearly with iterations."
               % (phase1, phase2, self.ITER))
        # phase2 observed as 1-24 on CPython 3.14 debug when clean;
        # 100 is a generous ceiling that still catches any leak
        # producing more than ~0.05 refs/call.
        self.assertLess(abs(phase2), 100, msg)

    @skip_if_speedups_missing
    def test_dumps_no_leak(self):
        data = {"a": [1, 2, 3], "b": "hello", "c": None, "d": True}
        self._assert_no_leak(lambda: simplejson.dumps(data))

    @skip_if_speedups_missing
    def test_loads_no_leak(self):
        raw = '{"a": [1, 2, 3], "b": "hello", "c": null, "d": true}'
        self._assert_no_leak(lambda: simplejson.loads(raw))

    @skip_if_speedups_missing
    def test_scanner_construction_no_leak(self):
        self._assert_no_leak(lambda: simplejson.JSONDecoder())

    @skip_if_speedups_missing
    def test_encoder_construction_no_leak(self):
        self._assert_no_leak(lambda: simplejson.JSONEncoder())

    @skip_if_speedups_missing
    def test_failed_construction_no_leak(self):
        """Error path in scanner_new/encoder_new must release module_ref."""
        class BadBool:
            def __bool__(self):
                raise ZeroDivisionError()
            __nonzero__ = __bool__

        def try_bad_scanner():
            try:
                decoder.JSONDecoder(strict=BadBool()).decode('{}')
            except ZeroDivisionError:
                pass

        def try_bad_encoder():
            try:
                encoder.JSONEncoder(skipkeys=BadBool()).encode({})
            except ZeroDivisionError:
                pass

        self._assert_no_leak(try_bad_scanner)
        self._assert_no_leak(try_bad_encoder)

    @skip_if_speedups_missing
    def test_circular_reference_no_leak(self):
        """ValueError mid-encode must not leak the partial accumulator,
        markers dict entry, or the ident PyLong."""
        def circular():
            d = {}
            d["self"] = d
            try:
                simplejson.dumps(d)
            except ValueError:
                pass
        self._assert_no_leak(circular)

    @skip_if_speedups_missing
    def test_asdict_returning_non_dict_no_leak(self):
        """encoder_steal_encode's TypeError path on _asdict() returning
        a non-dict must release the stolen newobj reference."""
        class BadNT:
            def _asdict(self):
                return "not a dict"

        def bad_asdict():
            try:
                simplejson.dumps(BadNT(), namedtuple_as_object=True)
            except TypeError:
                pass
        self._assert_no_leak(bad_asdict)

    @skip_if_speedups_missing
    def test_for_json_raising_no_leak(self):
        """for_json() raising inside its body must not leak the method
        binding or partial accumulator state."""
        class Explodes:
            def for_json(self):
                raise RuntimeError("boom")

        def explode():
            try:
                simplejson.dumps(Explodes(), for_json=True)
            except RuntimeError:
                pass
        self._assert_no_leak(explode)

    @skip_if_speedups_missing
    def test_non_string_dict_keys_no_leak(self):
        """Dict keys that aren't already strings go through
        encoder_stringify_key and the non-cached branch of the key_memo
        logic. Both paths must release the transient stringified key."""
        data = {1: "a", 2: "b", 3: "c", True: "x", False: "y"}
        self._assert_no_leak(lambda: simplejson.dumps(data, sort_keys=True))

    @skip_if_speedups_missing
    def test_bigint_as_string_no_leak(self):
        """maybe_quote_bigint's comparison path must release `encoded`
        on the RichCompareBool error branch and on the quoted-return
        path that replaces the unquoted string."""
        big = 1 << 40
        self._assert_no_leak(
            lambda: simplejson.dumps(big, int_as_string_bitcount=31))

    @skip_if_speedups_missing
    def test_dict_fast_path_no_leak(self):
        """The PyDict_Next fast path (unsorted exact dict) must not leak
        references compared to the iterator slow path."""
        data = {"a": 1, "b": "two", "c": [3], "d": None, "e": True}
        self._assert_no_leak(lambda: simplejson.dumps(data))

    @skip_if_speedups_missing
    def test_dict_slow_path_no_leak(self):
        """The iterator slow path (sorted or dict subclass) must not leak."""
        data = {"z": 1, "a": 2, "m": 3}
        self._assert_no_leak(
            lambda: simplejson.dumps(data, sort_keys=True))

    @skip_if_speedups_missing
    def test_skipkeys_fast_path_no_leak(self):
        """skipkeys on the PyDict_Next fast path must not leak skipped keys."""
        data = {"ok": 1, 42: 2, True: 3, None: 4}
        self._assert_no_leak(
            lambda: simplejson.dumps(data, skipkeys=True))

    @skip_if_speedups_missing
    def test_list_fast_path_no_leak(self):
        """The indexed fast path for exact lists must not leak."""
        data = [1, "two", 3.0, True, None, [4], {"k": "v"}]
        self._assert_no_leak(lambda: simplejson.dumps(data))

    @skip_if_speedups_missing
    def test_tuple_fast_path_no_leak(self):
        """The indexed fast path for exact tuples must not leak."""
        data = (1, "two", 3.0, True, None)
        self._assert_no_leak(
            lambda: simplejson.dumps(data, tuple_as_array=True))

