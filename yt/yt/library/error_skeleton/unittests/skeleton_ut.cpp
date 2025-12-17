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

    TStringBuf expectedSkeleton = "#1: foo @ [#2: bar; #3: baz; #4: qux @ [#5: quux]]";
    EXPECT_EQ(
        expectedSkeleton,
        GetErrorSkeleton(error));
    EXPECT_EQ(
        expectedSkeleton,
        error.GetSkeleton());
}

class TTestReplacement
    : public testing::TestWithParam<std::tuple<std::string, std::string>>
{ };

TEST_P(TTestReplacement, TestReplacement)
{
    auto error = TError(TErrorCode(42), std::get<0>(GetParam()), TError::TDisableFormat{});
    auto expectedSkeleton = std::string("#42: ") + std::get<1>(GetParam());

    EXPECT_EQ(
        expectedSkeleton,
        GetErrorSkeleton(error));
}

INSTANTIATE_TEST_SUITE_P(
    TTestReplacement,
    TTestReplacement,
    ::testing::Values(
        std::tuple{
            "foo; bar 123-abc-987654-fed //home some-node.yp-c.yandex.net:1234 0-0-0-0",
            "foo bar <guid> <path> <address> <guid>",
        },
        std::tuple{
            "Key \"hello\" with key \"some_other-key\" and attribute \"my-attr-42\" not found for Account \"some-my_account\" with timestamp 186be60dc00027fe",
            "Key <key> with key <key> and attribute <attribute> not found for Account <account> with timestamp <timestamp>",
        },
        std::tuple{
            "Undefined reference \"v1\" and reference \"HelloWorld.QQQQQ\"",
            "Undefined reference <reference> and reference <reference>",
        },
        std::tuple{
             "Error validating column \"my_column_name\"",
             "Error validating column <column>",
        },
        std::tuple{
            "Invalid mount revision of tablet <guid>: expected 2ccea0009dcf7, received 2cce1000a62e3",
            "Invalid mount revision of tablet <guid>: expected <hex>, received <hex>",
        },
        std::tuple{
            "abcdef01-76b7118a-9b8a9c91-beea1a0f 112233bb cafebab 0xdeadbeef",
            "<guid> <hex> cafebab 0x<hex>",
        },
        std::tuple{
            "Access denied for user \"robot-yt\"",
            "Access denied for user <user>",
        }
    )
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

