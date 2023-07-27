import unittest
import sys

sys.path.insert(0, "../dynamic")

import pybindexample


class TestModule(unittest.TestCase):
    def testModule(self):
        self.assertTrue(pybindexample.true_func())

    def testLambdaField(self):
        obj = pybindexample.derived()
        self.assertEqual(obj.lambda_field, 1)
        self.assertEqual(obj.lambda_method(), "abacaba")
        self.assertTrue(obj.lambda_method2())

    def testIterator(self):
        obj = pybindexample.complex("1")
        it = obj.get_iterator()
        x = [y for y in it]
        self.assertEqual(x, [0, 1, 2, 3])

    def testSetGet(self):
        check_str = "888999"
        pybindexample.set_name(check_str)
        self.assertEqual(check_str, pybindexample.get_name())

    def testClasses(self):
        complexx = pybindexample.complex("1")
        self.assertEqual(complexx.get(1).f, 1)
        self.assertEqual(complexx.get("2").f2, "2")
        self.assertIsNone(complexx.do_nothing())

    def testDerived(self):
        derived = pybindexample.derived()
        self.assertEqual(derived.newField, "aba")
        self.assertEqual(derived.f, -1)
        self.assertEqual(isinstance(derived, pybindexample.simple), True)
        derivedClass = pybindexample.derived_class()
        self.assertEqual(derivedClass.get_f(), 0)

    def testFromStruct(self):
        complexx = pybindexample.complex("")
        some_struct = complexx.get("x")
        complexx.from_struct(some_struct)
        self.assertEqual(complexx.get_name(), "x")

    def testThrow(self):
        try:
            pybindexample.complex("").throw0()
        except Exception as ex:
            self.assertTrue(-1 != str(ex).find("try to catch me"))
            return

        self.assertTrue(False)

    def testThrow1Arg(self):
        try:
            pybindexample.complex("").throw1(1)
        except Exception as ex:
            self.assertTrue(-1 != str(ex).find("catch 1 arg"))
            return

        self.assertTrue(False)

    def testRawInitClass(self):
        obj = pybindexample.raw_init_class(443, b="sd")
        self.assertEqual(obj.a, 443)
        self.assertEqual(obj.b, "sd")
