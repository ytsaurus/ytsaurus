import sys
from unittest import TestCase

import simplejson as json
import simplejson.decoder
from simplejson.compat import b, PY3

class TestScanString(TestCase):
    # The bytes type is intentionally not used in most of these tests
    # under Python 3 because the decoder immediately coerces to str before
    # calling scanstring. In Python 2 we are testing the code paths
    # for both unicode and str.
    #
    # The reason this is done is because Python 3 would require
    # entirely different code paths for parsing bytes and str.
    #
    def test_py_scanstring(self):
        self._test_scanstring(simplejson.decoder.py_scanstring)

    def test_c_scanstring(self):
        if not simplejson.decoder.c_scanstring:
            return
        self._test_scanstring(simplejson.decoder.c_scanstring)

        self.assertTrue(isinstance(simplejson.decoder.c_scanstring('""', 0)[0], str))

    def _test_scanstring(self, scanstring):
        if sys.maxunicode == 65535:
            self.assertEqual(
                scanstring(u'"z\U0001d120x"', 1, None, True),
                (u'z\U0001d120x', 6))
        else:
            self.assertEqual(
                scanstring(u'"z\U0001d120x"', 1, None, True),
                (u'z\U0001d120x', 5))

        self.assertEqual(
            scanstring('"\\u007b"', 1, None, True),
            (u'{', 8))

        self.assertEqual(
            scanstring('"A JSON payload should be an object or array, not a string."', 1, None, True),
            (u'A JSON payload should be an object or array, not a string.', 60))

        self.assertEqual(
            scanstring('["Unclosed array"', 2, None, True),
            (u'Unclosed array', 17))

        self.assertEqual(
            scanstring('["extra comma",]', 2, None, True),
            (u'extra comma', 14))

        self.assertEqual(
            scanstring('["double extra comma",,]', 2, None, True),
            (u'double extra comma', 21))

        self.assertEqual(
            scanstring('["Comma after the close"],', 2, None, True),
            (u'Comma after the close', 24))

        self.assertEqual(
            scanstring('["Extra close"]]', 2, None, True),
            (u'Extra close', 14))

        self.assertEqual(
            scanstring('{"Extra comma": true,}', 2, None, True),
            (u'Extra comma', 14))

        self.assertEqual(
            scanstring('{"Extra value after close": true} "misplaced quoted value"', 2, None, True),
            (u'Extra value after close', 26))

        self.assertEqual(
            scanstring('{"Illegal expression": 1 + 2}', 2, None, True),
            (u'Illegal expression', 21))

        self.assertEqual(
            scanstring('{"Illegal invocation": alert()}', 2, None, True),
            (u'Illegal invocation', 21))

        self.assertEqual(
            scanstring('{"Numbers cannot have leading zeroes": 013}', 2, None, True),
            (u'Numbers cannot have leading zeroes', 37))

        self.assertEqual(
            scanstring('{"Numbers cannot be hex": 0x14}', 2, None, True),
            (u'Numbers cannot be hex', 24))

        self.assertEqual(
            scanstring('[[[[[[[[[[[[[[[[[[[["Too deep"]]]]]]]]]]]]]]]]]]]]', 21, None, True),
            (u'Too deep', 30))

        self.assertEqual(
            scanstring('{"Missing colon" null}', 2, None, True),
            (u'Missing colon', 16))

        self.assertEqual(
            scanstring('{"Double colon":: null}', 2, None, True),
            (u'Double colon', 15))

        self.assertEqual(
            scanstring('{"Comma instead of colon", null}', 2, None, True),
            (u'Comma instead of colon', 25))

        self.assertEqual(
            scanstring('["Colon instead of comma": false]', 2, None, True),
            (u'Colon instead of comma', 25))

        self.assertEqual(
            scanstring('["Bad value", truth]', 2, None, True),
            (u'Bad value', 12))

        for c in map(chr, range(0x00, 0x1f)):
            self.assertEqual(
                scanstring(c + '"', 0, None, False),
                (c, 2))
            self.assertRaises(
                ValueError,
                scanstring, c + '"', 0, None, True)

        self.assertRaises(ValueError, scanstring, '', 0, None, True)
        self.assertRaises(ValueError, scanstring, 'a', 0, None, True)
        self.assertRaises(ValueError, scanstring, '\\', 0, None, True)
        self.assertRaises(ValueError, scanstring, '\\u', 0, None, True)
        self.assertRaises(ValueError, scanstring, '\\u0', 0, None, True)
        self.assertRaises(ValueError, scanstring, '\\u01', 0, None, True)
        self.assertRaises(ValueError, scanstring, '\\u012', 0, None, True)
        self.assertRaises(ValueError, scanstring, '\\u0123', 0, None, True)
        if sys.maxunicode > 65535:
            self.assertRaises(ValueError,
                              scanstring, '\\ud834\\u"', 0, None, True)
            self.assertRaises(ValueError,
                              scanstring, '\\ud834\\x0123"', 0, None, True)

        self.assertRaises(json.JSONDecodeError, scanstring, '\\u-123"', 0, None, True)
        # SJ-PT-23-01: Invalid Handling of Broken Unicode Escape Sequences
        self.assertRaises(json.JSONDecodeError, scanstring, '\\u EDD"', 0, None, True)

    def test_issue3623(self):
        self.assertRaises(ValueError, json.decoder.scanstring, "xxx", 1,
                          "xxx")
        self.assertRaises(UnicodeDecodeError,
                          json.encoder.encode_basestring_ascii, b("xx\xff"))

    def test_overflow(self):
        # Python 2.5 does not have maxsize, Python 3 does not have maxint
        maxsize = getattr(sys, 'maxsize', getattr(sys, 'maxint', None))
        assert maxsize is not None
        self.assertRaises(OverflowError, json.decoder.scanstring, "xxx",
                          maxsize + 1)

    def test_end_out_of_bounds_is_jsondecodeerror(self):
        # Regression: C scanstring used to raise a plain ValueError for
        # out-of-range end indices, while py_scanstring raises
        # JSONDecodeError. User code with `except JSONDecodeError:` missed
        # the C path. Both backends now raise JSONDecodeError with the
        # "Unterminated string starting at" message at pos = end - 1.
        for s, end in (
            (u'"abc"', 100),
            (u'abc', 100),
            (u'', 100),
            (u'abc', -1),
            (u'', -1),
        ):
            with self.assertRaises(json.JSONDecodeError) as cm:
                json.decoder.scanstring(s, end, None, True)
            self.assertEqual(cm.exception.pos, end - 1,
                             'scanstring(%r, %r) pos=%r, expected %r' %
                             (s, end, cm.exception.pos, end - 1))
            self.assertIn('Unterminated string', str(cm.exception))

    def test_surrogates(self):
        scanstring = json.decoder.scanstring

        def assertScan(given, expect, test_utf8=True):
            givens = [given]
            if not PY3 and test_utf8:
                givens.append(given.encode('utf8'))
            for given in givens:
                (res, count) = scanstring(given, 1, None, True)
                self.assertEqual(len(given), count)
                self.assertEqual(res, expect)

        assertScan(
            u'"z\\ud834\\u0079x"',
            u'z\ud834yx')
        assertScan(
            u'"z\\ud834\\udd20x"',
            u'z\U0001d120x')
        assertScan(
            u'"z\\ud834\\ud834\\udd20x"',
            u'z\ud834\U0001d120x')
        assertScan(
            u'"z\\ud834x"',
            u'z\ud834x')
        assertScan(
            u'"z\\udd20x"',
            u'z\udd20x')
        assertScan(
            u'"z\ud834x"',
            u'z\ud834x')
        # It may look strange to join strings together, but Python is drunk.
        # https://gist.github.com/etrepum/5538443
        assertScan(
            u'"z\\ud834\udd20x12345"',
            u''.join([u'z\ud834', u'\udd20x12345']))
        assertScan(
            u'"z\ud834\\udd20x"',
            u''.join([u'z\ud834', u'\udd20x']))
        # these have different behavior given UTF8 input, because the surrogate
        # pair may be joined (in maxunicode > 65535 builds)
        assertScan(
            u''.join([u'"z\ud834', u'\udd20x"']),
            u''.join([u'z\ud834', u'\udd20x']),
            test_utf8=False)

        self.assertRaises(ValueError,
                          scanstring, u'"z\\ud83x"', 1, None, True)
        self.assertRaises(ValueError,
                          scanstring, u'"z\\ud834\\udd2x"', 1, None, True)

    def test_escape_error_parity(self):
        # Regression: the C scanstring bounds check was `end >= len` / the
        # surrogate-pair bounds check was `end + 6 < len`. Both were
        # off-by-one, causing C to raise "Invalid \\uXXXX escape sequence"
        # where pure-Python correctly raised "Unterminated string starting
        # at" when a \\uXXXX escape used the last bytes of the buffer. The
        # error-position offset also differed: C reported the position of
        # the 'u' while Python reported the position of the leading '\'.
        # This test asserts exact parity (exception class, position, and
        # message prefix) across a matrix of edge cases.
        if simplejson.decoder.c_scanstring is None:
            return

        def get_exc(scanstring, s):
            try:
                scanstring(s, 0, None, True)
            except json.JSONDecodeError as e:
                return (e.pos, str(e).split(':')[0])
            return None

        # Each case: (input, expected_pos, expected_message_prefix)
        # expected_pos == -2 means (-1, 'Unterminated string starting at');
        # otherwise the positional 'Invalid \\uXXXX escape sequence' error.
        UNTERMINATED = (-1, 'Unterminated string starting at')
        def INVALID(pos):
            return (pos, 'Invalid \\uXXXX escape sequence')

        cases = [
            # Not enough room for 4 hex digits after \u.
            (u'\\u', INVALID(0)),
            (u'\\u0', INVALID(0)),
            (u'\\u01', INVALID(0)),
            (u'\\u012', INVALID(0)),
            # 4 non-hex chars after \u — C used to raise at the 'u'.
            (u'\\uXXXX', INVALID(0)),
            # Exactly 4 hex digits at buffer end — C used to mis-report
            # 'Invalid \\uXXXX escape' instead of 'Unterminated string'.
            (u'\\u0123', UNTERMINATED),
            # Lone high surrogate with no room for a second escape.
            (u'\\ud834', UNTERMINATED),
            # High surrogate followed by a truncated second escape.
            (u'\\ud834\\u', INVALID(6)),
            (u'\\ud834\\ux', INVALID(6)),
            (u'\\ud834\\udd2', INVALID(6)),
            (u'\\ud834\\udd2x', INVALID(6)),
            # High surrogate followed by a valid low surrogate that ends
            # exactly at the buffer edge — must combine before the outer
            # loop reports an unterminated string.
            (u'\\ud834\\udd1e', UNTERMINATED),
            (u'prefix\\ud834\\udd1e', UNTERMINATED),
        ]
        for s, expected in cases:
            py = get_exc(simplejson.decoder.py_scanstring, s)
            c = get_exc(simplejson.decoder.c_scanstring, s)
            self.assertEqual(py, expected,
                             'py_scanstring(%r) expected %r, got %r' %
                             (s, expected, py))
            self.assertEqual(c, expected,
                             'c_scanstring(%r) expected %r, got %r' %
                             (s, expected, c))

        # Success paths: valid escape or surrogate pair ending at the
        # closing quote must still parse correctly.
        for scanstring in (simplejson.decoder.py_scanstring,
                           simplejson.decoder.c_scanstring):
            self.assertEqual(
                scanstring(u'\\u0123"', 0, None, True),
                (u'\u0123', 7))
            self.assertEqual(
                scanstring(u'\\ud834\\udd1e"', 0, None, True),
                (u'\U0001d11e', 13))
