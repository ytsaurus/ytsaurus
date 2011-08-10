#!/usr/bin/python

import sys

def to_camelcase(s):
    return "".join(x.title() for x in s.split("_"))

def main():
    def usage():
        print >>sys.stderr, "  Usage: {0} testcase method1 method2 method3 ...".format(sys.argv[0])
        print >>sys.stderr, "Example: {0} microwave cook bake boil".format(sys.argv[0])
        sys.exit(1)

    if len(sys.argv) < 2:
        usage()

    generate_test(sys.argv[1], sys.argv[2:])

def generate_test(testcase, methods):
    with open("{0}_ut.cpp".format(testcase), "w") as handle:
        print >>handle, """
// XXX: Add includes here.
// #include "../ytlib/xxx/yyy.h"

#include "framework/framework.h"

namespace NYT {{

////////////////////////////////////////////////////////////////////////////////
// XXX: Place all fixture types here.
////////////////////////////////////////////////////////////////////////////////

""".lstrip("\r\n").rstrip("\r\n")

        for method in methods:
            print >>handle, """
TEST(T{testcase}Test, {method})
{{
    // XXX: Implement me.
    FAIL();
}}
""".rstrip("\r\n").format(
    method = to_camelcase(method),
    testcase = to_camelcase(testcase)
)

        print >>handle, """
////////////////////////////////////////////////////////////////////////////////

}} // namespace NYT
""".rstrip("\r\n").format(testcase = to_camelcase(testcase))
        print >>handle, ""

if __name__ == "__main__":
    main()
