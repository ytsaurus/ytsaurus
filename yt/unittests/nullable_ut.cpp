#include "stdafx.h"

#include <ytlib/misc/nullable.h>
#include <ytlib/misc/property.h>

#include <contrib/testing/framework.h>

namespace NYT {

using ::ToString;

#ifndef NDEBUG
TEST(TNullableDeathTest, Uninitialized)
{
    TNullable<int> nullable;
    EXPECT_FALSE(nullable.HasValue());
    ASSERT_DEATH(nullable.Get(), ".*");
}
#endif

inline void TestNullable(const TNullable<int>& nullable, bool initialized, int value = 0)
{
    EXPECT_EQ(initialized, nullable.HasValue());
    if (initialized) {
        EXPECT_EQ(value, nullable.Get());
    }
}

TEST(TNullableTest, Construct)
{
    TestNullable(TNullable<int>(), false);
    TestNullable(TNullable<int>(1), true, 1);
    TestNullable(TNullable<int>(true, 1), true, 1);
    TestNullable(TNullable<int>(false, 1), false);
    TestNullable(TNullable<int>(TNullable<double>()), false);
    TestNullable(TNullable<int>(TNullable<double>(1.1)), true, 1);
}

TEST(TNullableTest, AssignReset)
{
    TNullable<int> nullable;
    TestNullable(nullable, false);
    TestNullable(nullable = 1, true, 1);
    TestNullable(nullable = TNullable<double>(), false);
    TestNullable(nullable = TNullable<double>(1.1), true, 1);
    nullable.Reset();
    TestNullable(nullable, false);
}

TEST(TNullableTest, Destruct)
{
    TNullable<std::vector<int>> nullable(std::vector<int>(1));
    const auto* ptr = nullable.GetPtr();
    YASSERT(ptr->size() == 1);
    nullable.Reset();
    EXPECT_EQ(0, ptr->size());
}

inline void TestSwap(TNullable<int> nullable1, TNullable<int> nullable2)
{
    bool initialized1 = nullable1.HasValue();
    int value1 = initialized1 ? *nullable1 : 0;
    bool initialized2 = nullable2.HasValue();
    int value2 = initialized2 ? *nullable2 : 0;

    nullable1.Swap(nullable2);

    TestNullable(nullable1, initialized2, value2);
    TestNullable(nullable2, initialized1, value1);
}

TEST(TNullableTest, Swap)
{
    TestSwap(TNullable<int>(1), TNullable<int>(2));
    TestSwap(TNullable<int>(1), TNullable<int>());
    TestSwap(TNullable<int>(), TNullable<int>(2));
    TestSwap(TNullable<int>(), TNullable<int>());
}

TEST(TNullableTest, GetValueOrDefault)
{
    EXPECT_EQ(2, TNullable<int>(2).Get(1));
    EXPECT_EQ(1, TNullable<int>().Get(1));
}

TEST(TNullableTest, MakeNullable)
{
    TestNullable(MakeNullable(1), true, 1);
    TestNullable(MakeNullable(true, 1), true, 1);
    TestNullable(MakeNullable(false, 1), false);
}

TEST(TNullableTest, Null)
{
    TestNullable(Null, false);
    // TestNullable(NULL, true, 0); // Doesn't compile with gcc (ambigous conversion). Don't use this.
}

TEST(TNullableTest, Operators)
{
    {
        TNullable<std::vector<int>> nullable(std::vector<int>(1));
        EXPECT_EQ(1, (*nullable).size());
        EXPECT_EQ(1, nullable->size());
        EXPECT_EQ(&nullable.Get(), nullable.GetPtr());
        EXPECT_TRUE(nullable);
        nullable.Reset();
        EXPECT_FALSE(nullable);
    }
    {
        EXPECT_EQ(TNullable<int>(1), TNullable<int>(1));
        EXPECT_EQ(TNullable<int>(1), 1);
        EXPECT_EQ(TNullable<int>(), TNullable<int>());
        EXPECT_NE(TNullable<int>(1), TNullable<int>(2));
        EXPECT_NE(TNullable<int>(1), 2);
        EXPECT_NE(TNullable<int>(1), TNullable<int>());
        EXPECT_NE(TNullable<int>(), TNullable<int>(2));
    }
}

TEST(TNullableTest, ToString)
{
    EXPECT_EQ(ToString(MakeNullable(1)), "1");
    EXPECT_EQ(ToString(TNullable<int>()), "<Null>");
}

}
