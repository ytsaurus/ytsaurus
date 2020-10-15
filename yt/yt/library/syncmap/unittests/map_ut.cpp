#include <gtest/gtest.h>

#include <yt/library/syncmap/map.h>

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

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NConcurrency
