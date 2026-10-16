import sys, pickle
import unittest
from unittest import TestCase

import simplejson as json
from simplejson.compat import text_type, b

class TestErrors(TestCase):
    def test_string_keys_error(self):
        data = [{'a': 'A', 'b': (2, 4), 'c': 3.0, ('d',): 'D tuple'}]
        try:
            json.dumps(data)
        except TypeError:
            err = sys.exc_info()[1]
        else:
            self.fail('Expected TypeError')
        self.assertEqual(str(err),
                'keys must be str, int, float, bool or None, not tuple')

    def test_not_serializable(self):
        try:
            json.dumps(json)
        except TypeError:
            err = sys.exc_info()[1]
        else:
            self.fail('Expected TypeError')
        self.assertEqual(str(err),
                'Object of type module is not JSON serializable')

    def test_decode_error(self):
        err = None
        try:
            json.loads('{}\na\nb')
        except json.JSONDecodeError:
            err = sys.exc_info()[1]
        else:
            self.fail('Expected JSONDecodeError')
        self.assertEqual(err.lineno, 2)
        self.assertEqual(err.colno, 1)
        self.assertEqual(err.endlineno, 3)
        self.assertEqual(err.endcolno, 2)

    def test_scan_error(self):
        err = None
        for t in (text_type, b):
            try:
                json.loads(t('{"asdf": "'))
            except json.JSONDecodeError:
                err = sys.exc_info()[1]
            else:
                self.fail('Expected JSONDecodeError')
            self.assertEqual(err.lineno, 1)
            self.assertEqual(err.colno, 10)

    def test_error_is_pickable(self):
        err = None
        try:
            json.loads('{}\na\nb')
        except json.JSONDecodeError:
            err = sys.exc_info()[1]
        else:
            self.fail('Expected JSONDecodeError')
        s = pickle.dumps(err)
        e = pickle.loads(s)

        self.assertEqual(err.msg, e.msg)
        self.assertEqual(err.doc, e.doc)
        self.assertEqual(err.pos, e.pos)
        self.assertEqual(err.end, e.end)

    @unittest.skipIf(sys.version_info < (3, 11), 'add_note requires Python 3.11+')
    def test_add_note_list_recursion(self):
        x = []
        x.append(x)
        try:
            json.dumps(x)
        except ValueError as exc:
            self.assertEqual(
                exc.__notes__, ['when serializing list item 0'])
        else:
            self.fail('Expected ValueError')

    @unittest.skipIf(sys.version_info < (3, 11), 'add_note requires Python 3.11+')
    def test_add_note_dict_recursion(self):
        x = {}
        x['test'] = x
        try:
            json.dumps(x)
        except ValueError as exc:
            self.assertEqual(
                exc.__notes__, ["when serializing dict item 'test'"])
        else:
            self.fail('Expected ValueError')

    @unittest.skipIf(sys.version_info < (3, 11), 'add_note requires Python 3.11+')
    def test_add_note_nested_error(self):
        try:
            json.dumps({'a': [1, object(), 3]})
        except TypeError as exc:
            self.assertEqual(len(exc.__notes__), 3)
            self.assertEqual(exc.__notes__[0],
                'when serializing object object')
            self.assertEqual(exc.__notes__[1],
                'when serializing list item 1')
            self.assertEqual(exc.__notes__[2],
                "when serializing dict item 'a'")
        else:
            self.fail('Expected TypeError')
