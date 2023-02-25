#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/library/error_skeleton/skeleton.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TEST(TSkeletonTest, TestSimple)
{
    auto error =
        TError(TErrorCode(1), "foo")
            << TError(TErrorCode(2), "bar")
            << TError(TErrorCode(3), "baz")
            << TError(TErrorCode(2), "bar")
            << (TError(TErrorCode(4), "qux")
                << TError(TErrorCode(5), "quux"))
            << TError(TErrorCode(3), "baz");

    TString expectedSkeleton = "#1: foo @ [#2: bar; #3: baz; #4: qux @ [#5: quux]]";
    EXPECT_EQ(
        expectedSkeleton,
        GetErrorSkeleton(error));
    EXPECT_EQ(
        expectedSkeleton,
        error.GetSkeleton());
}

TEST(TSkeletonTest, TestReplacement)
{
    {
        auto error = TError(TErrorCode(42), "foo; bar 123-abc-987654-fed //home some-node.yp-c.yandex.net:1234 0-0-0-0");

        TString expectedSkeleton = "#42: foo bar <guid> <path> <address> <guid>";
        EXPECT_EQ(
            expectedSkeleton,
            GetErrorSkeleton(error));
    }

    {
        auto error = TError(
            TErrorCode(42),
            "Key \"hello\" with key \"some_other-key\" and attribute \"my-attr-42\" not found for Account \"some-my_account\" with timestamp 186be60dc00027fe");

        TString expectedSkeleton = "#42: Key <key> with key <key> and attribute <attribute> not found for Account <account> with timestamp <timestamp>";
        EXPECT_EQ(
            expectedSkeleton,
            GetErrorSkeleton(error));
    }

    {
        auto error = TError(TErrorCode(42), "Undefined reference \"v1\"");

        TString expectedSkeleton = "#42: Undefined reference <reference>";
        EXPECT_EQ(
            expectedSkeleton,
            GetErrorSkeleton(error));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

