#include <library/cpp/yt/small_containers/compact_queue.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <queue>
#include <random>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TCompactQueueTest, Simple)
{
    TCompactQueue<int, 4> queue;
    queue.push(1);
    queue.push(2);
    queue.push(3);

    for (int i = 1; i <= 10; i++) {
        EXPECT_EQ(queue.size(), 3u);
        EXPECT_EQ(queue.front(), i);
        EXPECT_EQ(queue.pop(), i);
        queue.push(i + 3);
    }

    for (int i = 11; i <= 13; i++) {
        EXPECT_EQ(queue.front(), i);
        queue.pop();
    }

    EXPECT_TRUE(queue.empty());
}

TEST(TCompactQueueTest, Reallocation1)
{
    TCompactQueue<int, 2> queue;
    queue.push(1);
    queue.push(2);
    queue.push(3);

    for (int i = 1; i <= 10; i++) {
        EXPECT_EQ(queue.size(), 3u);
        EXPECT_EQ(queue.front(), i);
        EXPECT_EQ(queue.pop(), i);
        queue.push(i + 3);
    }

    for (int i = 11; i <= 13; i++) {
        EXPECT_EQ(queue.front(), i);
        queue.pop();
    }

    EXPECT_TRUE(queue.empty());
}

TEST(TCompactQueueTest, Reallocation2)
{
    TCompactQueue<int, 3> queue;
    queue.push(1);
    queue.push(2);
    queue.push(3);
    EXPECT_EQ(queue.pop(), 1);
    queue.push(4);
    queue.push(5);

    EXPECT_EQ(queue.size(), 4u);

    for (int i = 2; i <= 10; i++) {
        EXPECT_EQ(queue.size(), 4u);
        EXPECT_EQ(queue.front(), i);
        EXPECT_EQ(queue.pop(), i);
        queue.push(i + 4);
    }

    for (int i = 11; i <= 14; i++) {
        EXPECT_EQ(queue.front(), i);
        queue.pop();
    }

    EXPECT_TRUE(queue.empty());
}

TEST(TCompactQueueTest, Stress)
{
    std::mt19937_64 rng(42);

    for (int iteration = 0; iteration < 1000; ++iteration) {
        TCompactQueue<int, 4> queue1;
        std::queue<int> queue2;

        for (int step = 0; step < 10'000; ++step) {
            EXPECT_EQ(queue1.size(), queue2.size());
            EXPECT_EQ(queue1.empty(), queue2.empty());
            if (!queue1.empty()) {
                EXPECT_EQ(queue1.front(), queue2.front());
            }

            if (queue2.empty() || rng() % 2 == 0) {
                int value = rng() % 1'000'000;
                queue1.push(value);
                queue2.push(value);
            } else {
                queue1.pop();
                queue2.pop();
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
