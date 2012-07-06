#include "stdafx.h"

#include <ytlib/misc/random.h>

#include <contrib/testing/framework.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TEST(TRandomGeneratorTest, DifferentTypes)
{
    TRandomGenerator rg1(100500);
    TRandomGenerator rg2(100500);

    EXPECT_EQ(rg1.GetNext<ui64>(), rg2.GetNext<ui64>());
    EXPECT_EQ(rg1.GetNext<i64>(),  rg2.GetNext<i64>());
    EXPECT_EQ(rg1.GetNext<ui32>(), rg2.GetNext<ui32>());
    EXPECT_EQ(rg1.GetNext<i32>(),  rg2.GetNext<i32>());
    EXPECT_EQ(rg1.GetNext<char>(), rg2.GetNext<char>());

    EXPECT_EQ(rg1.GetNext<double>(), rg2.GetNext<double>());
}

TEST(TRandomGeneratorTest, Many)
{
    TRandomGenerator rg1(100500);
    TRandomGenerator rg2(100500);

    for (int i = 0; i < 1000; ++i) {
        EXPECT_EQ(rg1.GetNext<ui64>(), rg2.GetNext<ui64>());
    }
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
