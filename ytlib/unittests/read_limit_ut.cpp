#include <yt/core/test_framework/framework.h>

#include <yt/client/chunk_client/read_limit.h>

#include <yt/core/ytree/convert.h>

namespace NYT::NChunkClient {
namespace {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

template <typename T>
TString DumpToYson(T obj)
{
    return ConvertToYsonString(obj, NYson::EYsonFormat::Text).GetData();
}

TEST(TReadLimitTest, Simple)
{
    TReadLimit limit;
    EXPECT_EQ("{}", DumpToYson(limit));
    EXPECT_TRUE(limit.IsTrivial());

    limit.SetRowIndex(0);
    EXPECT_EQ("{\"row_index\"=0;}", DumpToYson(limit));
    EXPECT_FALSE(limit.IsTrivial());
}

TEST(TReadRangeTest, Simple)
{
    TReadRange range;
    EXPECT_EQ("{}", DumpToYson(range));
}

TEST(TReadRangeTest, Simple2)
{
    auto range = ConvertTo<TReadRange>(TYsonString("{lower_limit={row_index=1};upper_limit={row_index=2}}"));
    EXPECT_EQ(1, range.LowerLimit().GetRowIndex());
    EXPECT_EQ(2, range.UpperLimit().GetRowIndex());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChunkClient

