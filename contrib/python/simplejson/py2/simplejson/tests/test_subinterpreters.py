"""Tests that verify the C extension works in subinterpreters.

Subinterpreters became usable for third-party C extensions in
Python 3.12 (PEP 684). On 3.13+ the extension uses per-module state
and heap types so that each interpreter gets its own copy.
"""
import sys
import unittest
from unittest import TestCase

from ._helpers import skip_if_speedups_missing


@unittest.skipIf(sys.version_info < (3, 12),
                 "subinterpreters require Python 3.12+")
class TestSubinterpreters(TestCase):
    """Test that the C extension can be loaded in subinterpreters."""

    def _run_in_subinterp(self, code):
        """Helper to run code in a fresh subinterpreter."""
        try:
            import _interpreters
        except ImportError:
            raise unittest.SkipTest("_interpreters not available")
        interp = _interpreters.create()
        try:
            _interpreters.run_string(interp, code)
        finally:
            _interpreters.destroy(interp)

    @skip_if_speedups_missing
    def test_import_in_subinterpreter(self):
        """Verify _speedups can be imported in a subinterpreter."""
        self._run_in_subinterp(
            "import simplejson; simplejson.dumps({'a': 1})")

    @skip_if_speedups_missing
    def test_encode_in_subinterpreter(self):
        """Verify encoding works correctly in a subinterpreter."""
        self._run_in_subinterp("""
import simplejson
assert simplejson.dumps(None) == 'null'
assert simplejson.dumps(True) == 'true'
assert simplejson.dumps(False) == 'false'
assert simplejson.dumps(42) == '42'
assert simplejson.dumps(3.14) == '3.14'
assert simplejson.dumps("hello") == '"hello"'
assert simplejson.dumps([1, 2, 3]) == '[1, 2, 3]'
assert simplejson.dumps({"a": 1}, sort_keys=True) == '{"a": 1}'
""")

    @skip_if_speedups_missing
    def test_decode_in_subinterpreter(self):
        """Verify decoding works correctly in a subinterpreter."""
        self._run_in_subinterp("""
import simplejson
assert simplejson.loads('null') is None
assert simplejson.loads('true') is True
assert simplejson.loads('42') == 42
assert simplejson.loads('"hello"') == 'hello'
assert simplejson.loads('[1, 2, 3]') == [1, 2, 3]
assert simplejson.loads('{"a": 1}') == {"a": 1}
""")

    @skip_if_speedups_missing
    def test_multiple_subinterpreters(self):
        """Verify multiple subinterpreters can use simplejson concurrently."""
        try:
            import _interpreters
        except ImportError:
            raise unittest.SkipTest("_interpreters not available")
        interps = [_interpreters.create() for _ in range(3)]
        try:
            for i, interp in enumerate(interps):
                _interpreters.run_string(interp, """
import simplejson
result = simplejson.dumps({"interp": %d})
assert '"interp": %d' in result
""" % (i, i))
        finally:
            for interp in interps:
                _interpreters.destroy(interp)

    @skip_if_speedups_missing
    def test_subinterpreter_state_independent(self):
        """Verify destroying one subinterpreter doesn't affect another."""
        try:
            import _interpreters
        except ImportError:
            raise unittest.SkipTest("_interpreters not available")
        interp1 = _interpreters.create()
        interp2 = _interpreters.create()
        try:
            # Both interpreters load and use simplejson
            _interpreters.run_string(interp1,
                "import simplejson; simplejson.dumps([1])")
            _interpreters.run_string(interp2,
                "import simplejson; simplejson.dumps([2])")

            # Destroy the first interpreter
            _interpreters.destroy(interp1)
            interp1 = None

            # Second interpreter must still work correctly
            _interpreters.run_string(interp2, """
import simplejson
assert simplejson.dumps({"still": "works"}) == '{"still": "works"}'
assert simplejson.loads('{"still": "works"}') == {"still": "works"}
""")
        finally:
            if interp1 is not None:
                _interpreters.destroy(interp1)
            _interpreters.destroy(interp2)

    @skip_if_speedups_missing
    @unittest.skipIf(sys.version_info < (3, 13),
                     "heap types require Python 3.13+")
    def test_subinterpreter_heap_types(self):
        """Verify types are heap types inside subinterpreters."""
        self._run_in_subinterp("""
from simplejson._speedups import make_scanner, make_encoder
# Py_TPFLAGS_HEAPTYPE = 1 << 9
assert make_scanner.__flags__ & (1 << 9), "Scanner should be heap type"
assert make_encoder.__flags__ & (1 << 9), "Encoder should be heap type"
""")
