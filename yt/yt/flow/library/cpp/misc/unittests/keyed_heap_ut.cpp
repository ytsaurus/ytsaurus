#include <yt/yt/flow/library/cpp/misc/keyed_heap.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TTestKeyedHeapTest, TBasicTest)
{
    TKeyedHeap<std::string, int> heap;
    EXPECT_TRUE(heap.Empty());
    ASSERT_EQ(heap.Size(), 0u);

    heap.Set("second", 2);
    heap.Set("first", 1);

    ASSERT_EQ(heap.TopValue(), 1);
    ASSERT_EQ(heap.Size(), 2u);

    heap.Erase("first");
    ASSERT_EQ(heap.TopValue(), 2);
    ASSERT_EQ(heap.Size(), 1u);

    heap.Set("third", 3);
    ASSERT_EQ(heap.TopValue(), 2);
    ASSERT_EQ(heap.Size(), 2u);

    EXPECT_FALSE(heap.Erase("non-existent"));
    ASSERT_EQ(heap.Size(), 2u);

    heap.Clear();
    ASSERT_EQ(heap.Size(), 0u);
    EXPECT_TRUE(heap.Empty());
}

TEST(TTestKeyedHeapTest, TCustomComparer)
{
    auto comparer = [] (int left, int right) {
        return left * left > right * right;
    };
    TKeyedHeap<std::string, int, decltype(comparer)> heap;

    heap.Set("first", 0);
    heap.Set("second", 1);
    heap.Set("third", 10);
    heap.Set("fourth", -5);
    heap.Set("fifth", 11);
    heap.Set("third", -10);

    ASSERT_EQ(heap.TopValue(), 11);
    heap.Erase("fifth");
    ASSERT_EQ(heap.TopValue(), -10);
    heap.Erase("third");
    ASSERT_EQ(heap.TopValue(), -5);
    heap.Erase("fourth");
    ASSERT_EQ(heap.TopValue(), 1);
    heap.Erase("second");
    ASSERT_EQ(heap.TopValue(), 0);
}

// Thank you @getsiu
TEST(TTestKeyedHeapTest, TOrdering)
{
    TKeyedHeap<int, int, std::less<int>> heap;

    heap.Set(1, 1);
    heap.Set(2, 2);
    heap.Set(5, 5);
    heap.Set(3, 3);
    heap.Set(6, 6);
    heap.Set(7, 7);
    heap.Set(8, 8);
    heap.Set(4, 4);

    heap.Erase(8);
    ASSERT_EQ(heap.TopValue(), 1);

    heap.Set(8, 8);
    heap.Erase(1);
    ASSERT_EQ(heap.TopValue(), 2);

    heap.Set(9, 9);
    heap.Erase(2);
    ASSERT_EQ(heap.TopValue(), 3);

    heap.Set(10, 10);
    heap.Erase(3);
    ASSERT_EQ(heap.TopValue(), 4);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
