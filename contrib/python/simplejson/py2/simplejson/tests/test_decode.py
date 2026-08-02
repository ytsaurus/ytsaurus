from __future__ import absolute_import
import decimal
from unittest import TestCase

import sys
import simplejson as json
from simplejson.compat import StringIO, b, binary_type
from simplejson import OrderedDict

class MisbehavingBytesSubtype(binary_type):
    def decode(self, encoding=None):
        return "bad decode"
    def __str__(self):
        return "bad __str__"
    def __bytes__(self):
        return b("bad __bytes__")

class TestDecode(TestCase):
    def test_decimal(self):
        rval = json.loads('1.1', parse_float=decimal.Decimal)
        self.assertTrue(isinstance(rval, decimal.Decimal))
        self.assertEqual(rval, decimal.Decimal('1.1'))

    def test_float(self):
        rval = json.loads('1', parse_int=float)
        self.assertTrue(isinstance(rval, float))
        self.assertEqual(rval, 1.0)

    def test_decoder_optimizations(self):
        # Several optimizations were made that skip over calls to
        # the whitespace regex, so this test is designed to try and
        # exercise the uncommon cases. The array cases are already covered.
        rval = json.loads('{   "key"    :    "value"    ,  "k":"v"    }')
        self.assertEqual(rval, {"key":"value", "k":"v"})

    def test_empty_objects(self):
        s = '{}'
        self.assertEqual(json.loads(s), eval(s))
        s = '[]'
        self.assertEqual(json.loads(s), eval(s))
        s = '""'
        self.assertEqual(json.loads(s), eval(s))

    def test_object_pairs_hook(self):
        s = '{"xkd":1, "kcw":2, "art":3, "hxm":4, "qrt":5, "pad":6, "hoy":7}'
        p = [("xkd", 1), ("kcw", 2), ("art", 3), ("hxm", 4),
             ("qrt", 5), ("pad", 6), ("hoy", 7)]
        self.assertEqual(json.loads(s), eval(s))
        self.assertEqual(json.loads(s, object_pairs_hook=lambda x: x), p)
        self.assertEqual(json.load(StringIO(s),
                                   object_pairs_hook=lambda x: x), p)
        od = json.loads(s, object_pairs_hook=OrderedDict)
        self.assertEqual(od, OrderedDict(p))
        self.assertEqual(type(od), OrderedDict)
        # the object_pairs_hook takes priority over the object_hook
        self.assertEqual(json.loads(s,
                                    object_pairs_hook=OrderedDict,
                                    object_hook=lambda x: None),
                         OrderedDict(p))

    def check_keys_reuse(self, source, loads):
        rval = loads(source)
        (a, b), (c, d) = sorted(rval[0]), sorted(rval[1])
        self.assertIs(a, c)
        self.assertIs(b, d)

    def test_keys_reuse_str(self):
        s = u'[{"a_key": 1, "b_\xe9": 2}, {"a_key": 3, "b_\xe9": 4}]'.encode('utf8')
        self.check_keys_reuse(s, json.loads)

    def test_keys_reuse_unicode(self):
        s = u'[{"a_key": 1, "b_\xe9": 2}, {"a_key": 3, "b_\xe9": 4}]'
        self.check_keys_reuse(s, json.loads)

    def test_empty_strings(self):
        self.assertEqual(json.loads('""'), "")
        self.assertEqual(json.loads(u'""'), u"")
        self.assertEqual(json.loads('[""]'), [""])
        self.assertEqual(json.loads(u'[""]'), [u""])

    def test_raw_decode(self):
        cls = json.decoder.JSONDecoder
        self.assertEqual(
            ({'a': {}}, 9),
            cls().raw_decode("{\"a\": {}}"))
        # http://code.google.com/p/simplejson/issues/detail?id=85
        self.assertEqual(
            ({'a': {}}, 9),
            cls(object_pairs_hook=dict).raw_decode("{\"a\": {}}"))
        # https://github.com/simplejson/simplejson/pull/38
        self.assertEqual(
            ({'a': {}}, 11),
            cls().raw_decode(" \n{\"a\": {}}"))

    def test_bytes_decode(self):
        cls = json.decoder.JSONDecoder
        data = b('"\xe2\x82\xac"')
        self.assertEqual(cls().decode(data), u'\u20ac')
        self.assertEqual(cls(encoding='latin1').decode(data), u'\xe2\x82\xac')
        self.assertEqual(cls(encoding=None).decode(data), u'\u20ac')

        data = MisbehavingBytesSubtype(b('"\xe2\x82\xac"'))
        self.assertEqual(cls().decode(data), u'\u20ac')
        self.assertEqual(cls(encoding='latin1').decode(data), u'\xe2\x82\xac')
        self.assertEqual(cls(encoding=None).decode(data), u'\u20ac')

    def test_bounds_checking(self):
        # https://github.com/simplejson/simplejson/issues/98
        j = json.decoder.JSONDecoder()
        for i in [4, 5, 6, -1, -2, -3, -4, -5, -6]:
            self.assertRaises(ValueError, j.scan_once, '1234', i)
            self.assertRaises(ValueError, j.raw_decode, '1234', i)
        x, y = sorted(['128931233', '472389423'], key=id)
        diff = id(x) - id(y)
        self.assertRaises(ValueError, j.scan_once, y, diff)
        self.assertRaises(ValueError, j.raw_decode, y, i)

    def test_array_hook(self):
        result = json.loads('[1, [2, 3], 4]', array_hook=tuple)
        self.assertEqual(result, (1, (2, 3), 4))
        self.assertIsInstance(result, tuple)

    def test_array_hook_empty(self):
        result = json.loads('[]', array_hook=tuple)
        self.assertEqual(result, ())
        self.assertIsInstance(result, tuple)

    def test_array_hook_nested_in_object(self):
        result = json.loads('{"a": [1, 2]}', array_hook=tuple)
        self.assertEqual(result, {"a": (1, 2)})

    def test_array_hook_with_object_hook(self):
        # Both hooks applied together: array_hook on arrays,
        # object_hook on objects
        def freeze_dict(d):
            return tuple(sorted(d.items()))
        s = '{"xkd": [[1], [2], [3]]}'
        result = json.loads(s, object_hook=freeze_dict, array_hook=tuple)
        self.assertIsInstance(result, tuple)
        # object_hook converts dict to sorted tuple of items
        xkd_value = dict(result)['xkd']
        self.assertIsInstance(xkd_value, tuple)
        for item in xkd_value:
            self.assertIsInstance(item, tuple)

    def test_array_hook_none(self):
        # array_hook=None should behave like no hook
        result = json.loads('[1, 2]', array_hook=None)
        self.assertEqual(result, [1, 2])
        self.assertIsInstance(result, list)

    def test_trailing_comma_unexpected_data(self):
        # Matches CPython's test_unexpected_data for trailing commas
        test_cases = [
            ('[42,]', 'Illegal trailing comma before end of array'),
            ('{"spam":42,}', 'Illegal trailing comma before end of object'),
            ('{"spam":42 , }', 'Illegal trailing comma before end of object'),
            ('[123  , ]', 'Illegal trailing comma before end of array'),
        ]
        for doc, expected_msg in test_cases:
            try:
                json.loads(doc)
                self.fail("Expected JSONDecodeError for %r" % (doc,))
            except json.JSONDecodeError as e:
                self.assertIn(expected_msg, str(e),
                    "Wrong error for %r: %s" % (doc, e))

    def test_nonascii_digits_rejected(self):
        # Non-ASCII digits (e.g. fullwidth digits) must not be accepted
        # as JSON numbers. Matches CPython gh-125687.
        for num in [u"1\uff10", u"0.\uff10", u"0e\uff10"]:
            self.assertRaises(json.JSONDecodeError, json.loads, num)

    def test_bounded_int(self):
        # SJ-PT-23-03, limit quadratic number parsing per Python 3.11
        max_str_digits = getattr(sys, 'get_int_max_str_digits', lambda: 4300)()
        s = '1' + '0' * (max_str_digits - 1)
        self.assertEqual(json.loads(s), int(s))
        self.assertRaises(ValueError, json.loads, s + '0')
