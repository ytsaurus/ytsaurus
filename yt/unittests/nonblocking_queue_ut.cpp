#include "stdafx.h"
#include "framework.h"

#include <core/concurrency/nonblocking_queue.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

TEST(TNonblockingQueueTest, DequeueFirst)
{
    TNonblockingQueue<int> queue;
    auto result1 = queue.Dequeue();
    auto result2 = queue.Dequeue();

    EXPECT_FALSE(result1.IsSet());
    EXPECT_FALSE(result2.IsSet());

    queue.Enqueue(1);

    EXPECT_TRUE(result1.IsSet());
    EXPECT_EQ(1, result1.Get().Value());

    queue.Enqueue(2);

    EXPECT_TRUE(result2.IsSet());
    EXPECT_EQ(2, result2.Get().Value());
}

TEST(TNonblockingQueueTest, EnqueueFirst)
{
    TNonblockingQueue<int> queue;
    queue.Enqueue(1);
    queue.Enqueue(2);

    auto result1 = queue.Dequeue();
    EXPECT_TRUE(result1.IsSet());
    EXPECT_EQ(1, result1.Get().Value());

    auto result2 = queue.Dequeue();
    EXPECT_TRUE(result2.IsSet());
    EXPECT_EQ(2, result2.Get().Value());
}

TEST(TNonblockingQueueTest, Mixed)
{
    TNonblockingQueue<int> queue;
    queue.Enqueue(1);

    auto result1 = queue.Dequeue();
    EXPECT_TRUE(result1.IsSet());
    EXPECT_EQ(1, result1.Get().Value());

    auto result2 = queue.Dequeue();
    EXPECT_FALSE(result2.IsSet());

    queue.Enqueue(2);
    EXPECT_TRUE(result2.IsSet());
    EXPECT_EQ(2, result2.Get().Value());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

