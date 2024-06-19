#include <yt/yt/server/lib/tablet_balancer/bounded_priority_queue.h>

#include <yt/yt/core/test_framework/framework.h>

#include <random>

namespace NYT::NTabletBalancer {
namespace {

////////////////////////////////////////////////////////////////////////////////

std::vector<int> GenerateRandomSequence(int length, int minValue = 1, int maxValue = 100)
{
    std::mt19937 gen(2365);
    std::uniform_int_distribution<> distributor(minValue, maxValue);
    std::vector<int> data(length);
    for (auto& d : data) {
        d = distributor(gen);
    }
    return data;
}

class TBoundedPriorityQueueTest
    : public ::testing::Test
{ };

TEST(TBoundedPriorityQueueTest, IsEmpty)
{
    TBoundedPriorityQueue<int> priorityQueue{1};
    ASSERT_TRUE(priorityQueue.IsEmpty());
    priorityQueue.Insert(1, 1);
    ASSERT_FALSE(priorityQueue.IsEmpty());
    auto result = priorityQueue.ExtractMax();
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(priorityQueue.IsEmpty());
}

TEST(TBoundedPriorityQueueTest, InvalidatingIterator)
{
    TBoundedPriorityQueue<int> priorityQueue{2};
    priorityQueue.Insert(1, 1);
    priorityQueue.Insert(2, 2);
    auto result = priorityQueue.ExtractMax();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value().second, 2);
    ASSERT_TRUE(priorityQueue.ExtractMax().has_value());
    ASSERT_TRUE(priorityQueue.IsEmpty());
}

TEST(TBoundedPriorityQueueTest, CorrectSize)
{
    TBoundedPriorityQueue<int> priorityQueue{3};
    priorityQueue.Insert(1, 100);
    priorityQueue.Insert(2, 200);
    priorityQueue.Insert(3, 300);
    priorityQueue.Insert(4, 400);
    priorityQueue.Insert(5, 500);
    priorityQueue.Insert(6, 600);

    EXPECT_EQ(priorityQueue.ExtractMax().value().second, 600);
    EXPECT_EQ(priorityQueue.ExtractMax().value().second, 500);
    EXPECT_EQ(priorityQueue.ExtractMax().value().second, 400);
    ASSERT_TRUE(priorityQueue.IsEmpty());
}

TEST(TBoundedPriorityQueueTest, CorrectDisplacement)
{
    TBoundedPriorityQueue<int> priorityQueue{2};
    priorityQueue.Insert(1, 100);
    priorityQueue.Insert(3, 300);
    priorityQueue.Insert(2, 200);
    priorityQueue.Insert(4, 100);

    EXPECT_EQ(priorityQueue.ExtractMax().value().second, 100);
    EXPECT_EQ(priorityQueue.ExtractMax().value().second, 300);
    ASSERT_TRUE(priorityQueue.IsEmpty());
}

TEST(TBoundedPriorityQueueTest, Invalidate)
{
    TBoundedPriorityQueue<int> priorityQueue{5};
    priorityQueue.Insert(1, 101);
    priorityQueue.Insert(2, 102);
    priorityQueue.Insert(3, 103);
    priorityQueue.Invalidate([] (const auto& item) {
        return item.first < 3;
    });
    EXPECT_EQ(priorityQueue.ExtractMax().value().second, 103);
    ASSERT_TRUE(priorityQueue.IsEmpty());
}

TEST(TBoundedPriorityQueueTest, BestDiscarded)
{
    TBoundedPriorityQueue<int> priorityQueue{1};
    priorityQueue.Insert(10, 100);
    priorityQueue.Insert(20, 200);
    auto result = priorityQueue.ExtractMax();
    EXPECT_EQ(result.value().second, 200);
    ASSERT_TRUE(priorityQueue.IsEmpty());
    priorityQueue.Insert(5, 50);
    ASSERT_TRUE(priorityQueue.IsEmpty());
}

TEST(TBoundedPriorityQueueTest, RandomSequences)
{
    size_t testSize = 100;
    auto randomData = GenerateRandomSequence(testSize);

    TBoundedPriorityQueue<int> priorityQueue(10);
    for (int num : randomData) {
        priorityQueue.Insert(num, num);
    }

    double lastValue = std::numeric_limits<double>::max();
    for (int i = 0; i < 10; ++i) {
        auto extractedValue = priorityQueue.ExtractMax().value().second;
        ASSERT_LE(extractedValue, lastValue);
        lastValue = extractedValue;
    }
}

TEST(TBoundedPriorityQueueTest, Reset)
{
    TBoundedPriorityQueue<int> priorityQueue(3);
    priorityQueue.Insert(1, 100);
    priorityQueue.Insert(2, 200);
    priorityQueue.Insert(3, 300);
    priorityQueue.Reset();
    ASSERT_TRUE(priorityQueue.IsEmpty());
}

TEST(TBoundedPriorityQueueTest, ResetAndPush)
{
    TBoundedPriorityQueue<int> priorityQueue(2);
    priorityQueue.Insert(1, 100);
    priorityQueue.Insert(2, 200);
    priorityQueue.Reset();
    ASSERT_TRUE(priorityQueue.IsEmpty());
    priorityQueue.Insert(1, 100);
    priorityQueue.Insert(2, 200);
    ASSERT_EQ(priorityQueue.ExtractMax().value().second, 200);
    ASSERT_EQ(priorityQueue.ExtractMax().value().second, 100);
    ASSERT_TRUE(priorityQueue.IsEmpty());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTabletBalancer
