import decimal
from decimal import Decimal
from unittest import TestCase
from simplejson.compat import StringIO, reload_module

import simplejson as json

class TestDecimal(TestCase):
    NUMS = "1.0", "10.00", "1.1", "1234567890.1234567890", "500"
    def dumps(self, obj, **kw):
        sio = StringIO()
        json.dump(obj, sio, **kw)
        res = json.dumps(obj, **kw)
        self.assertEqual(res, sio.getvalue())
        return res

    def loads(self, s, **kw):
        sio = StringIO(s)
        res = json.loads(s, **kw)
        self.assertEqual(res, json.load(sio, **kw))
        return res

    def test_decimal_encode(self):
        for d in map(Decimal, self.NUMS):
            self.assertEqual(self.dumps(d, use_decimal=True), str(d))

    def test_decimal_decode(self):
        for s in self.NUMS:
            self.assertEqual(self.loads(s, parse_float=Decimal), Decimal(s))

    def test_stringify_key(self):
        for d in map(Decimal, self.NUMS):
            v = {d: d}
            self.assertEqual(
                self.loads(
                    self.dumps(v, use_decimal=True), parse_float=Decimal),
                {str(d): d})

    def test_decimal_roundtrip(self):
        for d in map(Decimal, self.NUMS):
            # The type might not be the same (int and Decimal) but they
            # should still compare equal.
            for v in [d, [d], {'': d}]:
                self.assertEqual(
                    self.loads(
                        self.dumps(v, use_decimal=True), parse_float=Decimal),
                    v)

    def test_decimal_defaults(self):
        d = Decimal('1.1')
        # use_decimal=True is the default
        self.assertRaises(TypeError, json.dumps, d, use_decimal=False)
        self.assertEqual('1.1', json.dumps(d))
        self.assertEqual('1.1', json.dumps(d, use_decimal=True))
        self.assertRaises(TypeError, json.dump, d, StringIO(),
                          use_decimal=False)
        sio = StringIO()
        json.dump(d, sio)
        self.assertEqual('1.1', sio.getvalue())
        sio = StringIO()
        json.dump(d, sio, use_decimal=True)
        self.assertEqual('1.1', sio.getvalue())

    def test_decimal_reload(self):
        # Simulate a subinterpreter that reloads the Python modules but not
        # the C code https://github.com/simplejson/simplejson/issues/34
        global Decimal
        Decimal = reload_module(decimal).Decimal
        import simplejson.encoder
        simplejson.encoder.Decimal = Decimal
        self.test_decimal_roundtrip()

    def test_decimal_nan_allow(self):
        # Non-finite Decimals should be treated the same as the matching
        # float, i.e. emit the JavaScript literals when allow_nan is true.
        # https://github.com/simplejson/simplejson/issues/149
        self.assertEqual(json.dumps(Decimal('NaN'), allow_nan=True), 'NaN')
        self.assertEqual(
            json.dumps(Decimal('Infinity'), allow_nan=True), 'Infinity')
        self.assertEqual(
            json.dumps(Decimal('-Infinity'), allow_nan=True), '-Infinity')
        # sNaN is also not finite and must not leak through as a literal.
        self.assertEqual(json.dumps(Decimal('sNaN'), allow_nan=True), 'NaN')

    def test_decimal_nan_ignore(self):
        # ignore_nan should emit null for non-finite Decimals, matching float.
        # https://github.com/simplejson/simplejson/issues/149
        for d in (Decimal('NaN'), Decimal('sNaN'),
                  Decimal('Infinity'), Decimal('-Infinity')):
            self.assertEqual(json.dumps(d, ignore_nan=True), 'null')
        self.assertEqual(
            json.dumps([Decimal('0.33'), Decimal('NaN'), Decimal('0.20')],
                       ignore_nan=True),
            '[0.33, null, 0.20]')

    def test_decimal_nan_deny(self):
        # allow_nan=False must raise for non-finite Decimals rather than
        # silently emit invalid JSON.
        # https://github.com/simplejson/simplejson/issues/149
        for d in (Decimal('NaN'), Decimal('sNaN'),
                  Decimal('Infinity'), Decimal('-Infinity')):
            self.assertRaises(ValueError, json.dumps, d, allow_nan=False)
        # allow_nan defaults to False, so a plain dumps must raise too.
        self.assertRaises(ValueError, json.dumps, Decimal('NaN'))

    def test_decimal_nan_as_value(self):
        # Non-finite Decimals nested inside containers must be handled too.
        self.assertEqual(
            json.dumps({'a': Decimal('NaN')}, allow_nan=True), '{"a": NaN}')
        self.assertEqual(
            json.dumps({'a': Decimal('Infinity')}, ignore_nan=True),
            '{"a": null}')
        self.assertRaises(
            ValueError, json.dumps, [Decimal('NaN')], allow_nan=False)

    def test_decimal_nan_as_key(self):
        # A non-finite Decimal used as a dict key follows the same rules.
        self.assertEqual(
            json.dumps({Decimal('NaN'): 1}, allow_nan=True), '{"NaN": 1}')
        self.assertEqual(
            json.dumps({Decimal('Infinity'): 1}, ignore_nan=True),
            '{"null": 1}')
        self.assertRaises(
            ValueError, json.dumps, {Decimal('NaN'): 1}, allow_nan=False)

    def test_decimal_finite_unaffected(self):
        # The fix must not perturb finite Decimals, including zero and
        # values that would lose trailing zeros if routed through float().
        for s in ('0', '0.00', '-0', '1.50', '1E-30', '1234567890.1234567890'):
            d = Decimal(s)
            self.assertEqual(json.dumps(d), str(d))
            self.assertEqual(json.dumps(d, ignore_nan=True), str(d))
            self.assertEqual(json.dumps(d, allow_nan=True), str(d))
