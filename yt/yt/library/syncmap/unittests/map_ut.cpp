#include <gtest/gtest.h>

#include <yt/yt/library/syncmap/map.h>

namespace NYT::NConcurrency {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TSyncMap, SingleInsert)
{
    TSyncMap<int, int> map;

    auto ptr = map.Find(0);
    EXPECT_EQ(ptr, nullptr);

    auto [insertedPtr, inserted] = map.FindOrInsert(0, [] { return 42; });
    EXPECT_TRUE(inserted);
    EXPECT_EQ(42, *insertedPtr);

    for (int i = 0; i < 100; i++) {
        auto ptr = map.Find(0);
        EXPECT_EQ(42, *ptr);
    }
}

TEST(TSyncMap, TestInsertLoop)
{
    TSyncMap<int, int> map;

    for (int i = 0; i < 1000; ++i) {
        auto [insertedPtr, inserted] = map.FindOrInsert(i, [] { return 42; });
        EXPECT_TRUE(inserted);

        for (int j = 0; j < 1000; ++j) {
            auto ptr = map.Find(i);
            EXPECT_TRUE(ptr);
            EXPECT_EQ(*ptr, 42);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency
