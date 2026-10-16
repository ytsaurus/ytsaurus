#include <yt/yt/ytlib/chunk_client/erasure_helpers.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NChunkClient::NErasureHelpers {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TErasureHelpersTest, UnionPreservesFirstRangeBegin)
{
    auto result = Union({TPartRange{
        .Begin = 10,
        .End = 20,
    }});

    ASSERT_EQ(std::ssize(result), 1);
    EXPECT_EQ(result[0].Begin, 10);
    EXPECT_EQ(result[0].End, 20);
}

TEST(TErasureHelpersTest, UnionPreservesDisjointRanges)
{
    auto result = Union({
        TPartRange{
            .Begin = 30,
            .End = 40,
        },
        TPartRange{
            .Begin = 10,
            .End = 20,
        },
    });

    ASSERT_EQ(std::ssize(result), 2);
    EXPECT_EQ(result[0].Begin, 10);
    EXPECT_EQ(result[0].End, 20);
    EXPECT_EQ(result[1].Begin, 30);
    EXPECT_EQ(result[1].End, 40);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChunkClient::NErasureHelpers
