#include "ypath_join.h"

#include <library/unittest/registar.h>

using namespace NYT;

SIMPLE_UNIT_TEST_SUITE(JoinYPathsTestSuit)
{
    SIMPLE_UNIT_TEST(JoinNoArg)
    {
        UNIT_ASSERT_STRINGS_EQUAL(JoinYPaths(), "");
    }

    SIMPLE_UNIT_TEST(JoinOneArg)
    {
        UNIT_ASSERT_STRINGS_EQUAL(JoinYPaths("/"), "/");
        UNIT_ASSERT_STRINGS_EQUAL(JoinYPaths("///"), "///");

        UNIT_ASSERT_STRINGS_EQUAL(JoinYPaths("aa"), "aa");
        UNIT_ASSERT_STRINGS_EQUAL(JoinYPaths("///aa//"), "///aa//");
    }

    SIMPLE_UNIT_TEST(JoinTwoAgrs)
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

    SIMPLE_UNIT_TEST(JoinThreeAgrs)
    {
        UNIT_ASSERT_STRINGS_EQUAL(JoinYPaths("aa", "bb", "cc"), "aa/bb/cc");
        UNIT_ASSERT_STRINGS_EQUAL(JoinYPaths("aa", "/", "cc"), "//cc");
        UNIT_ASSERT_STRINGS_EQUAL(JoinYPaths("aa", "/b", "cc"), "aa/b/cc");
    }

    SIMPLE_UNIT_TEST(JoinException)
    {
        UNIT_ASSERT_STRINGS_EQUAL(JoinYPaths("\\/"), "\\/");
        UNIT_ASSERT_EXCEPTION(JoinYPaths("\\/", "a"), yexception);
        UNIT_ASSERT_STRINGS_EQUAL(JoinYPaths("\\//", "a"), "\\//a");
    }
}
