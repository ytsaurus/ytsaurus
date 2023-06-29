#include <util/generic/string.h> // should be before #include "ypath_join.h"
#include "ypath_join.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NYT;

Y_UNIT_TEST_SUITE(JoinYPathsTestSuit)
{
    Y_UNIT_TEST(JoinNoArg)
    {
        UNIT_ASSERT_STRINGS_EQUAL(JoinYPaths(), "");
    }

    Y_UNIT_TEST(JoinOneArg)
    {
        UNIT_ASSERT_STRINGS_EQUAL(JoinYPaths(TString{"/"}), "/");
        UNIT_ASSERT_STRINGS_EQUAL(JoinYPaths("///"), "///");

        UNIT_ASSERT_STRINGS_EQUAL(JoinYPaths("aa"), "aa");
        UNIT_ASSERT_STRINGS_EQUAL(JoinYPaths("///aa//"), "///aa//");
    }

    Y_UNIT_TEST(JoinTwoAgrs)
    {
        UNIT_ASSERT_STRINGS_EQUAL(JoinYPaths("aa", "bb"), "aa/bb");
        UNIT_ASSERT_STRINGS_EQUAL(JoinYPaths("aa/", "bb"), "aa/bb");
        UNIT_ASSERT_STRINGS_EQUAL(JoinYPaths("aa", "/bb"), "aa/bb");
        UNIT_ASSERT_STRINGS_EQUAL(JoinYPaths("aa/", "/bb"), "aa/bb");

        UNIT_ASSERT_STRINGS_EQUAL(JoinYPaths("///aa//", "bb"), "///aa//bb");
        UNIT_ASSERT_STRINGS_EQUAL(JoinYPaths("///aa//", "/bb/"), "///aa//bb/");
        UNIT_ASSERT_STRINGS_EQUAL(JoinYPaths("///aa//", "//bb//"), "//bb//");
        UNIT_ASSERT_STRINGS_EQUAL(JoinYPaths("///aa//", "///bb///"), "///bb///");
    }

    Y_UNIT_TEST(JoinThreeAgrs)
    {
        UNIT_ASSERT_STRINGS_EQUAL(JoinYPaths("aa", "bb", "cc"), "aa/bb/cc");
        UNIT_ASSERT_STRINGS_EQUAL(JoinYPaths("aa", "/", "cc"), "//cc");
        UNIT_ASSERT_STRINGS_EQUAL(JoinYPaths("aa", "/b", "cc"), "aa/b/cc");
    }

    Y_UNIT_TEST(JoinException)
    {
        UNIT_ASSERT_STRINGS_EQUAL(JoinYPaths("\\/"), "\\/");
        UNIT_ASSERT_EXCEPTION(JoinYPaths("\\/", "a"), yexception);
        UNIT_ASSERT_STRINGS_EQUAL(JoinYPaths("\\//", "a"), "\\//a");
    }
}
