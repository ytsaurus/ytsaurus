#include "stdafx.h"
#include "framework.h"

#include <core/concurrency/nonblocking_queue.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

typedef TNonblockingQueue<int> TIntQueue;

TEST(TNonblockingQueueTest, DequeueFirst)
{
    auto queue = New<TIntQueue>();
    auto result1 = queue->Dequeue();
    auto result2 = queue->Dequeue();

    EXPECT_FALSE(result1.IsSet());
    EXPECT_FALSE(result2.IsSet());

    queue->Enqueue(1);

    EXPECT_TRUE(result1.IsSet());
    EXPECT_EQ(1, result1.Get());

    queue->Enqueue(2);

    EXPECT_TRUE(result2.IsSet());
    EXPECT_EQ(2, result2.Get());
}

TEST(TNonblockingQueueTest, EnqueueFirst)
{
    auto queue = New<TIntQueue>();
    queue->Enqueue(1);
    queue->Enqueue(2);

    auto result1 = queue->Dequeue();
    EXPECT_TRUE(result1.IsSet());
    EXPECT_EQ(1, result1.Get());

    auto result2 = queue->Dequeue();
    EXPECT_TRUE(result2.IsSet());
    EXPECT_EQ(2, result2.Get());
}

TEST(TNonblockingQueueTest, Mixed)
{
    auto queue = New<TIntQueue>();
    queue->Enqueue(1);

    auto result1 = queue->Dequeue();
    EXPECT_TRUE(result1.IsSet());
    EXPECT_EQ(1, result1.Get());

    auto result2 = queue->Dequeue();
    EXPECT_FALSE(result2.IsSet());

    queue->Enqueue(2);
    EXPECT_TRUE(result2.IsSet());
    EXPECT_EQ(2, result2.Get());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

