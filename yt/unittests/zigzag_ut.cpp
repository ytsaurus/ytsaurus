#include "../ytlib/misc/zigzag.h"

#include <contrib/testing/framework.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TZigZagTest: public ::testing::Test
{ };

TEST_F(TZigZagTest, Encode32)
{
    EXPECT_EQ(ZigZagEncode32( 0), 0);
    EXPECT_EQ(ZigZagEncode32(-1), 1);
    EXPECT_EQ(ZigZagEncode32( 1), 2);
    EXPECT_EQ(ZigZagEncode32(-2), 3);
    // ...
    EXPECT_EQ(ZigZagEncode32(Max<i32>()), Max<ui32>() - 1);
    EXPECT_EQ(ZigZagEncode32(Min<i32>()), Max<ui32>());
}

TEST_F(TZigZagTest, Decode32)
{
    EXPECT_EQ(ZigZagDecode32(0),  0);
    EXPECT_EQ(ZigZagDecode32(1), -1);
    EXPECT_EQ(ZigZagDecode32(2),  1);
    EXPECT_EQ(ZigZagDecode32(3), -2);
    // ...
    EXPECT_EQ(ZigZagDecode32(Max<ui32>() - 1), Max<i32>());
    EXPECT_EQ(ZigZagDecode32(Max<ui32>()),     Min<i32>());
}

TEST_F(TZigZagTest, Encode64)
{
    EXPECT_EQ(ZigZagEncode64( 0), 0);
    EXPECT_EQ(ZigZagEncode64(-1), 1);
    EXPECT_EQ(ZigZagEncode64( 1), 2);
    EXPECT_EQ(ZigZagEncode64(-2), 3);
    // ...
    EXPECT_EQ(ZigZagEncode64(Max<i64>()), Max<ui64>() - 1);
    EXPECT_EQ(ZigZagEncode64(Min<i64>()), Max<ui64>());
}

TEST_F(TZigZagTest, Decode64)
{
    EXPECT_EQ(ZigZagDecode64(0),  0);
    EXPECT_EQ(ZigZagDecode64(1), -1);
    EXPECT_EQ(ZigZagDecode64(2),  1);
    EXPECT_EQ(ZigZagDecode64(3), -2);
    // ...
    EXPECT_EQ(ZigZagDecode64(Max<ui64>() - 1), Max<i64>());
    EXPECT_EQ(ZigZagDecode64(Max<ui64>()),     Min<i64>());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

