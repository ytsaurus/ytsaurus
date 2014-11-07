#include "stdafx.h"
#include "framework.h"

#include <ytlib/chunk_client/read_limit.h>

#include <core/ytree/convert.h>

namespace NYT {
namespace NChunkClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NYTree;

template<typename T>
Stroka DumpToYson(T obj)
{
    return ConvertToYsonString(obj, NYson::EYsonFormat::Text).Data();
}

TEST(TReadLimitTest, Simple)
{
    TReadLimit limit;
    EXPECT_EQ("{}", DumpToYson(limit));
    EXPECT_TRUE(limit.IsTrivial());

    limit.SetRowIndex(0);
    EXPECT_EQ("{\"row_index\"=0}", DumpToYson(limit));
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
} // namespace NChunkClient
} // namespace NYT

