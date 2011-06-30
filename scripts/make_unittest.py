#!/usr/bin/python

import sys

def to_camelcase(s):
    return "".join(x.title() for x in s.split("_"))

def main():
    def usage():
        print >>sys.stderr, "  Usage: {0} testcase method1 method2 method3 ...".format(sys.argv[0])
        print >>sys.stderr, "Example: {0} preprocessor concatenation for_each is_sequence"
        sys.exit(1)

    if len(sys.argv) < 2:
        usage()

    generate_test(sys.argv[1], sys.argv[2:])

def generate_test(testcase, methods):
    with open("{0}_ut.cpp".format(testcase), "w") as handle:
        print >>handle, """
// Add includes here.
// #include "../ytlib/xxx/yyy.h"

#include <library/unittest/registar.h>

namespace NYT {{

class T{testcase}Test
    : public TTestBase
{{
    UNIT_TEST_SUITE(T{testcase}Test);
""".lstrip("\r\n").rstrip("\r\n").format(testcase = to_camelcase(testcase))

        for method in methods:
            print >>handle, "        UNIT_TEST(Test{method});".format(method = to_camelcase(method))

        print >>handle, """
    UNIT_TEST_SUITE_END();

public:
""".lstrip("\r\n").rstrip("\r\n").format()

        for method in methods:
            print >>handle, """
    void Test{method}()
    {{
        // XXX: Implement me, please.
    }}
""".lstrip("\r\n").format(method = to_camelcase(method))

        print >>handle, """
}};

UNIT_TEST_SUITE_REGISTRATION(T{testcase}Test);

}} // namespace NYT

""".rstrip("\r\n").format(testcase = to_camelcase(testcase))

if __name__ == "__main__":
    main()
