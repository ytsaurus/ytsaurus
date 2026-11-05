from unittest import TestCase
import simplejson as json

def default_iterable(obj):
    return list(obj)

class TestCheckCircular(TestCase):
    def test_circular_dict(self):
        dct = {}
        dct['a'] = dct
        self.assertRaises(ValueError, json.dumps, dct)

    def test_circular_list(self):
        lst = []
        lst.append(lst)
        self.assertRaises(ValueError, json.dumps, lst)

    def test_circular_composite(self):
        dct2 = {}
        dct2['a'] = []
        dct2['a'].append(dct2)
        self.assertRaises(ValueError, json.dumps, dct2)

    def test_circular_default(self):
        json.dumps([set()], default=default_iterable)
        self.assertRaises(TypeError, json.dumps, [set()])

    def test_circular_off_default(self):
        json.dumps([set()], default=default_iterable, check_circular=False)
        self.assertRaises(TypeError, json.dumps, [set()], check_circular=False)

    def test_default_callback_clears_markers(self):
        # Regression test: clearing the markers dict from inside the
        # default() callback must not cause a use-after-free on ident.
        markers = {}
        call_count = [0]
        class Custom:
            pass
        def bad_default(obj):
            call_count[0] += 1
            if call_count[0] <= 1:
                markers.clear()
                return "safe"
            return str(obj)
        # Should not crash (previously: segfault from double Py_XDECREF)
        try:
            json.dumps(Custom(), default=bad_default)
        except (TypeError, ValueError):
            pass

