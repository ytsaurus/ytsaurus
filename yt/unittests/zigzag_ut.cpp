#include "stdafx.h"

#include "../ytlib/misc/zigzag.h"

#include <contrib/testing/framework.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TEST(TZigZagTest, Encode32)
{
    EXPECT_EQ(0u, ZigZagEncode32( 0));
    EXPECT_EQ(1u, ZigZagEncode32(-1));
    EXPECT_EQ(2u, ZigZagEncode32( 1));
    EXPECT_EQ(3u, ZigZagEncode32(-2));
    // ...
    EXPECT_EQ(Max<ui32>() - 1, ZigZagEncode32(Max<i32>()));
    EXPECT_EQ(Max<ui32>(),     ZigZagEncode32(Min<i32>()));
}

TEST(TZigZagTest, Decode32)
{
    EXPECT_EQ( 0, ZigZagDecode32(0));
    EXPECT_EQ(-1, ZigZagDecode32(1));
    EXPECT_EQ( 1, ZigZagDecode32(2));
    EXPECT_EQ(-2, ZigZagDecode32(3));
    // ...
    EXPECT_EQ(Max<i32>(), ZigZagDecode32(Max<ui32>() - 1));
    EXPECT_EQ(Min<i32>(), ZigZagDecode32(Max<ui32>()));
}

TEST(TZigZagTest, Encode64)
{
    EXPECT_EQ(0ull, ZigZagEncode64( 0));
    EXPECT_EQ(1ull, ZigZagEncode64(-1));
    EXPECT_EQ(2ull, ZigZagEncode64( 1));
    EXPECT_EQ(3ull, ZigZagEncode64(-2));
    // ...
    EXPECT_EQ(Max<ui64>() - 1, ZigZagEncode64(Max<i64>()));
    EXPECT_EQ(Max<ui64>(),     ZigZagEncode64(Min<i64>()));
}

TEST(TZigZagTest, Decode64)
{
    EXPECT_EQ(ZigZagDecode64(0),  0ll);
    EXPECT_EQ(ZigZagDecode64(1), -1ll);
    EXPECT_EQ(ZigZagDecode64(2),  1ll);
    EXPECT_EQ(ZigZagDecode64(3), -2ll);
    // ...
    EXPECT_EQ(Max<i64>(), ZigZagDecode64(Max<ui64>() - 1));
    EXPECT_EQ(Min<i64>(), ZigZagDecode64(Max<ui64>()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

