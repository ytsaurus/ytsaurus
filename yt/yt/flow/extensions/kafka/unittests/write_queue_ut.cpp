#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/extensions/kafka/sink.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

TKafkaMessageToWrite MakeMessage(i64 seqNo)
{
    return TKafkaMessageToWrite{
        .Key = std::nullopt,
        .Value = "payload-" + std::to_string(seqNo),
        .MessageId = std::nullopt,
    };
}

TFuture<void> Enqueue(TKafkaWriteQueue* queue, std::vector<TKafkaMessageToWrite> records)
{
    auto promise = NewPromise<void>();
    queue->Enqueue(std::move(records), BIND([promise] (const TError& error) {
        promise.Set(error);
    }));
    return promise.ToFuture();
}

//! Completion is synchronous, so the result is there without waiting on a fiber.
TError ResultOf(const TFuture<void>& future)
{
    auto result = future.TryGet();
    Y_ABORT_UNLESS(result, "future is not set");
    return *result;
}

////////////////////////////////////////////////////////////////////////////////

TEST(TKafkaWriteQueueTest, HandsPendingMessagesToTheProducer)
{
    TKafkaWriteQueue queue;

    auto message = MakeMessage(1);
    message.MessageId = "stream:0:42";
    message.Key = "key";
    auto firstFuture = Enqueue(&queue, {std::move(message)});
    auto secondFuture = Enqueue(&queue, {MakeMessage(2)});

    auto pending = queue.TakePending();
    ASSERT_EQ(std::ssize(pending), 2);
    EXPECT_EQ(pending[0].SeqNo, 1);
    EXPECT_EQ(pending[1].SeqNo, 2);
    EXPECT_EQ(pending[1].Value, "payload-2");
    EXPECT_EQ(pending[0].MessageId, "stream:0:42");
    EXPECT_EQ(pending[0].Key, "key");

    // The queue is drained; nothing is handed out twice.
    EXPECT_TRUE(queue.TakePending().empty());

    EXPECT_FALSE(firstFuture.IsSet());
    EXPECT_FALSE(secondFuture.IsSet());
}

TEST(TKafkaWriteQueueTest, HoldsBackOutOfOrderDeliveryReports)
{
    TKafkaWriteQueue queue;
    std::vector<int> completed;
    for (int index = 1; index <= 3; ++index) {
        queue.Enqueue({MakeMessage(index)}, BIND([&, index] (const TError& error) {
            EXPECT_TRUE(error.IsOK());
            // Observing the queue here also proves callbacks execute outside its lock.
            EXPECT_TRUE(queue.GetFatalError().IsOK());
            completed.push_back(index);
        }));
    }
    queue.Complete(3, TError());
    queue.Complete(2, TError());
    EXPECT_TRUE(completed.empty());
    queue.Complete(1, TError());
    EXPECT_EQ(completed, (std::vector{1, 2, 3}));
    auto futureFourth = Enqueue(&queue, {MakeMessage(4)});
    queue.Complete(1, TError("stale"));
    queue.Complete(100, TError("unknown"));
    EXPECT_FALSE(futureFourth.IsSet());
    EXPECT_TRUE(queue.GetFatalError().IsOK());
    EXPECT_EQ(completed, (std::vector{1, 2, 3}));
    queue.Complete(4, TError());
    EXPECT_TRUE(ResultOf(futureFourth).IsOK());
}

TEST(TKafkaWriteQueueTest, FailsEveryUnacknowledgedRequestOnAFailure)
{
    TKafkaWriteQueue queue;
    int failed = 0;
    auto callback = BIND([&] (const TError& error) {
        EXPECT_EQ(error.GetMessage(), "delivery failed");
        EXPECT_EQ(queue.GetFatalError().GetMessage(), "delivery failed");
        ++failed;
    });
    queue.Enqueue({MakeMessage(1)}, callback);
    queue.Enqueue(std::vector{MakeMessage(2), MakeMessage(2)}, callback);
    queue.Enqueue({MakeMessage(3)}, callback);
    queue.Complete(3, TError());
    queue.Complete(2, TError("delivery failed"));
    queue.Complete(2, TError());
    EXPECT_EQ(failed, 3);
    EXPECT_TRUE(queue.TakePending().empty());
    queue.Complete(1, TError());
    queue.Fail(TError("later failure"));
    queue.Enqueue({MakeMessage(4)}, callback);
    EXPECT_EQ(failed, 4);
    EXPECT_TRUE(queue.TakePending().empty());
}

TEST(TKafkaWriteQueueTest, FailsEveryOutstandingPromise)
{
    TKafkaWriteQueue queue;

    auto first = Enqueue(&queue, {MakeMessage(1)});
    auto second = Enqueue(&queue, {MakeMessage(2)});
    queue.Complete(1, TError());

    queue.Fail(TError("Kafka writer terminated"));
    queue.Complete(2, TError());

    EXPECT_TRUE(ResultOf(first).IsOK());
    ASSERT_TRUE(second.IsSet());
    EXPECT_FALSE(ResultOf(second).IsOK());
    EXPECT_EQ(ResultOf(second).GetMessage(), "Kafka writer terminated");
    EXPECT_TRUE(queue.TakePending().empty());
}

TEST(TKafkaWriteQueueTest, ResolvesAMultiRecordSeqNoOnTheLastAcknowledgement)
{
    TKafkaWriteQueue queue;

    // A split batch: three records acknowledged together under one seqNo.
    auto batch = Enqueue(&queue, std::vector{MakeMessage(1), MakeMessage(1), MakeMessage(1)});
    auto next = Enqueue(&queue, {MakeMessage(2)});
    auto pending = queue.TakePending(/*maxCount*/ 2, /*maxByteSize*/ 1'000);
    ASSERT_EQ(std::ssize(pending), 2);
    EXPECT_EQ(pending[0].SeqNo, pending[1].SeqNo);
    queue.Complete(1, TError());
    queue.Complete(1, TError());
    EXPECT_FALSE(batch.IsSet());

    pending = queue.TakePending(/*maxCount*/ 2, /*maxByteSize*/ 1'000);
    ASSERT_EQ(std::ssize(pending), 2);
    EXPECT_EQ(pending[0].SeqNo, 1);
    EXPECT_EQ(pending[1].SeqNo, 2);
    queue.Complete(2, TError());
    // The next seqNo may not resolve past a partially acknowledged batch.
    EXPECT_FALSE(batch.IsSet());
    EXPECT_FALSE(next.IsSet());

    queue.Complete(1, TError());
    EXPECT_TRUE(ResultOf(batch).IsOK());
    EXPECT_TRUE(ResultOf(next).IsOK());
}

TEST(TKafkaWriteQueueTest, TakesABoundedPrefix)
{
    TKafkaWriteQueue queue;
    for (i64 seqNo = 1; seqNo <= 5; ++seqNo) {
        Y_UNUSED(Enqueue(&queue, {MakeMessage(seqNo)}));
    }

    auto seqNos = [] (const std::deque<TKafkaMessageToWrite>& records) {
        std::vector<i64> result;
        for (const auto& record : records) {
            result.push_back(record.SeqNo);
        }
        return result;
    };
    // Each record carries nine bytes: "payload-N".
    EXPECT_EQ(seqNos(queue.TakePending(/*maxCount*/ 2, /*maxByteSize*/ 1'000)), (std::vector<i64>{1, 2}));
    EXPECT_EQ(seqNos(queue.TakePending(/*maxCount*/ 10, /*maxByteSize*/ 20)), (std::vector<i64>{3, 4}));
    // A record above the byte limit still goes, alone.
    EXPECT_EQ(seqNos(queue.TakePending(/*maxCount*/ 10, /*maxByteSize*/ 1)), (std::vector<i64>{5}));
    EXPECT_TRUE(queue.TakePending(/*maxCount*/ 10, /*maxByteSize*/ 1'000).empty());
}

TEST(TKafkaWriteQueueTest, CountsTheBytesOfARecord)
{
    auto record = TKafkaMessageToWrite{.SeqNo = 1, .Key = "key", .Value = "value", .MessageId = "id"};
    EXPECT_EQ(record.GetByteSize(), 10);

    // A tombstone without a key or a message id carries nothing.
    EXPECT_EQ(TKafkaMessageToWrite{.SeqNo = 1}.GetByteSize(), 0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
