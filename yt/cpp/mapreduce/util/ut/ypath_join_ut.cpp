#include <util/generic/string.h> // should be before #include "ypath_join.h"

#include <yt/cpp/mapreduce/util/ypath_join.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NYT;

TEST(TJoinYPathsTestSuit, JoinNoArg)
{
    ASSERT_EQ(JoinYPaths(), TString(""));
}

TEST(TJoinYPathsTestSuit, JoinOneArg)
{
    ASSERT_EQ(JoinYPaths(TString{"/"}), "/");
    ASSERT_EQ(JoinYPaths("///"), "///");

    ASSERT_EQ(JoinYPaths("aa"), "aa");
    ASSERT_EQ(JoinYPaths("///aa//"), "///aa//");
}

TEST(TJoinYPathsTestSuit, JoinTwoAgrs)
{
    ASSERT_EQ(JoinYPaths("aa", "bb"), "aa/bb");
    ASSERT_EQ(JoinYPaths("aa/", "bb"), "aa/bb");
    ASSERT_EQ(JoinYPaths("aa", "/bb"), "aa/bb");
    ASSERT_EQ(JoinYPaths("aa/", "/bb"), "aa/bb");

    ASSERT_EQ(JoinYPaths("///aa//", "bb"), "///aa//bb");
    ASSERT_EQ(JoinYPaths("///aa//", "/bb/"), "///aa//bb/");
    ASSERT_EQ(JoinYPaths("///aa//", "//bb//"), "//bb//");
    ASSERT_EQ(JoinYPaths("///aa//", "///bb///"), "///bb///");
}

TEST(TJoinYPathsTestSuit, JoinThreeAgrs)
{
    ASSERT_EQ(JoinYPaths("aa", "bb", "cc"), "aa/bb/cc");
    ASSERT_EQ(JoinYPaths("aa", "/", "cc"), "//cc");
    ASSERT_EQ(JoinYPaths("aa", "/b", "cc"), "aa/b/cc");
}

TEST(TJoinYPathsTestSuit, JoinException)
{
    ASSERT_EQ(JoinYPaths("\\/"), "\\/");
    EXPECT_THROW(JoinYPaths("\\/", "a"), yexception);
    ASSERT_EQ(JoinYPaths("\\//", "a"), "\\//a");
}
