from unittest import TestCase

import simplejson as json


class TestBitSizeIntAsString(TestCase):
    # Python 2.5, at least the one that ships on Mac OS X, calculates
    # 2 ** 31 as 0! It manages to calculate 1 << 31 correctly.
    values = [
        (200, 200),
        ((1 << 31) - 1, (1 << 31) - 1),
        ((1 << 31), str(1 << 31)),
        ((1 << 31) + 1, str((1 << 31) + 1)),
        (-100, -100),
        ((-1 << 31), str(-1 << 31)),
        ((-1 << 31) - 1, str((-1 << 31) - 1)),
        ((-1 << 31) + 1, (-1 << 31) + 1),
    ]

    def test_invalid_counts(self):
        for n in ['foo', -1, 0, 1.0]:
            self.assertRaises(
                TypeError,
                json.dumps, 0, int_as_string_bitcount=n)

    def test_ints_outside_range_fails(self):
        self.assertNotEqual(
            str(1 << 15),
            json.loads(json.dumps(1 << 15, int_as_string_bitcount=16)),
            )

    def test_ints(self):
        for val, expect in self.values:
            self.assertEqual(
                val,
                json.loads(json.dumps(val)))
            self.assertEqual(
                expect,
                json.loads(json.dumps(val, int_as_string_bitcount=31)),
                )

    def test_lists(self):
        for val, expect in self.values:
            val = [val, val]
            expect = [expect, expect]
            self.assertEqual(
                val,
                json.loads(json.dumps(val)))
            self.assertEqual(
                expect,
                json.loads(json.dumps(val, int_as_string_bitcount=31)))

    def test_dicts(self):
        for val, expect in self.values:
            val = {'k': val}
            expect = {'k': expect}
            self.assertEqual(
                val,
                json.loads(json.dumps(val)))
            self.assertEqual(
                expect,
                json.loads(json.dumps(val, int_as_string_bitcount=31)))

    def test_comparison_error_propagated(self):
        # Regression test for C extension bug: PyObject_RichCompareBool
        # returning -1 is truthy in C, causing the error to be silently
        # swallowed. The pure Python encoder uses < (not >=/<= via
        # RichCompareBool), so this only tests the C extension path.
        from simplejson import encoder
        if encoder.c_make_encoder is None:
            return
        class BadInt(int):
            def __ge__(self, other):
                raise RuntimeError("comparison bomb")
            def __le__(self, other):
                raise RuntimeError("comparison bomb")
        # Use bitcount=15 and value 2**16 so the value fits in a
        # Python 2 int on 32-bit platforms while still exceeding the
        # bitcount threshold.
        self.assertRaises(
            RuntimeError,
            json.dumps, BadInt(2**16), int_as_string_bitcount=15)

    def test_dict_keys(self):
        for val, _ in self.values:
            expect = {str(val): 'value'}
            val = {val: 'value'}
            self.assertEqual(
                expect,
                json.loads(json.dumps(val)))
            self.assertEqual(
                expect,
                json.loads(json.dumps(val, int_as_string_bitcount=31)))

    def test_boundary_at_max_bitcount(self):
        """Regression test for -Wshift-negative-value UB in encoder_new.

        The C implementation computes ``-(2 ** n)`` as the minimum
        stringifiable value; the naive ``-1LL << n`` expression is
        undefined behavior and overflows into LLONG_MIN incorrectly for
        n == 63. Exercise each bitcount from 1 to 63 with the exact
        boundary values to catch any regression in that computation.
        """
        max_n = 63
        for n in (1, 8, 31, 32, 62, max_n):
            boundary = 1 << n  # i.e. 2**n
            just_inside_pos = boundary - 1
            just_inside_neg = -boundary + 1
            # Values strictly inside [-2**n + 1, 2**n - 1] stay as ints
            for v in (0, 1, -1, just_inside_pos, just_inside_neg):
                self.assertEqual(
                    v,
                    json.loads(json.dumps(v, int_as_string_bitcount=n)),
                    "n=%d v=%d should stay an int" % (n, v))
            # Values at +/- 2**n get stringified (they're at the boundary)
            for v in (boundary, -boundary):
                self.assertEqual(
                    str(v),
                    json.loads(json.dumps(v, int_as_string_bitcount=n)),
                    "n=%d v=%d should be stringified" % (n, v))
