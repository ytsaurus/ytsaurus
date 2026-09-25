#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/extensions/kafka/sink.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

TKafkaMessageToWrite MakeMessage(i64 seqNo)
{
    return TKafkaMessageToWrite{
        .SeqNo = seqNo,
        .Key = std::nullopt,
        .Value = "payload-" + std::to_string(seqNo),
        .MessageId = std::nullopt,
    };
}

//! The queue resolves its promises synchronously, so the result is there without waiting on a fiber.
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

    auto firstFuture = queue.Enqueue(MakeMessage(1));
    auto secondFuture = queue.Enqueue(MakeMessage(2));

    auto pending = queue.TakePending();
    ASSERT_EQ(std::ssize(pending), 2);
    EXPECT_EQ(pending[0].SeqNo, 1);
    EXPECT_EQ(pending[1].SeqNo, 2);
    EXPECT_EQ(pending[1].Value, "payload-2");

    // The queue is drained; nothing is handed out twice.
    EXPECT_TRUE(queue.TakePending().empty());

    EXPECT_FALSE(firstFuture.IsSet());
    EXPECT_FALSE(secondFuture.IsSet());
}

TEST(TKafkaWriteQueueTest, ResolvesInSeqNoOrder)
{
    TKafkaWriteQueue queue;

    auto first = queue.Enqueue(MakeMessage(1));
    auto second = queue.Enqueue(MakeMessage(2));
    auto third = queue.Enqueue(MakeMessage(3));

    queue.Complete(1, TError());
    EXPECT_TRUE(first.IsSet());
    EXPECT_FALSE(second.IsSet());

    queue.Complete(2, TError());
    EXPECT_TRUE(second.IsSet());
    EXPECT_FALSE(third.IsSet());

    queue.Complete(3, TError());
    EXPECT_TRUE(third.IsSet());
}

TEST(TKafkaWriteQueueTest, HoldsBackOutOfOrderDeliveryReports)
{
    TKafkaWriteQueue queue;

    auto first = queue.Enqueue(MakeMessage(1));
    auto second = queue.Enqueue(MakeMessage(2));
    auto third = queue.Enqueue(MakeMessage(3));

    // Delivery reports may arrive in any order; the ordered sink base requires the futures to
    // complete in seqNo order regardless.
    queue.Complete(3, TError());
    queue.Complete(2, TError());
    EXPECT_FALSE(first.IsSet());
    EXPECT_FALSE(second.IsSet());
    EXPECT_FALSE(third.IsSet());

    queue.Complete(1, TError());
    EXPECT_TRUE(first.IsSet());
    EXPECT_TRUE(second.IsSet());
    EXPECT_TRUE(third.IsSet());
}

TEST(TKafkaWriteQueueTest, PoisonsEverySeqNoAfterAFailedOne)
{
    TKafkaWriteQueue queue;

    auto first = queue.Enqueue(MakeMessage(1));
    auto second = queue.Enqueue(MakeMessage(2));
    auto third = queue.Enqueue(MakeMessage(3));

    // Delivery reports arrive out of order; the third write even reports OK.
    queue.Complete(3, TError());
    queue.Complete(2, TError("Kafka delivery failed"));
    queue.Complete(1, TError());

    // Writes before the failure resolve truthfully.
    EXPECT_TRUE(ResultOf(first).IsOK());
    // The failed write carries its own error.
    EXPECT_FALSE(ResultOf(second).IsOK());
    // Everything after the failure fails despite the OK report: resolving it OK would let the sink
    // base persist past the failed message and deduplicate its replay away — silent data loss.
    EXPECT_FALSE(ResultOf(third).IsOK());

    // The poison is sticky: the queue is dead for the rest of the epoch.
    EXPECT_FALSE(queue.GetFatalError().IsOK());
    auto fourth = queue.Enqueue(MakeMessage(4));
    ASSERT_TRUE(fourth.IsSet());
    EXPECT_FALSE(ResultOf(fourth).IsOK());
    EXPECT_TRUE(queue.TakePending().empty());
}

TEST(TKafkaWriteQueueTest, PoisonsOutstandingWritesOnAFailure)
{
    TKafkaWriteQueue queue;

    auto first = queue.Enqueue(MakeMessage(1));
    auto second = queue.Enqueue(MakeMessage(2));

    // The first write fails while the second is still in flight; the second must not stay pending
    // forever nor ever resolve OK.
    queue.Complete(1, TError("Kafka delivery failed"));

    EXPECT_FALSE(ResultOf(first).IsOK());
    ASSERT_TRUE(second.IsSet());
    EXPECT_FALSE(ResultOf(second).IsOK());
}

TEST(TKafkaWriteQueueTest, IgnoresLateCompletionsAfterAFailure)
{
    TKafkaWriteQueue queue;

    auto first = queue.Enqueue(MakeMessage(1));
    queue.Fail(TError("Kafka writer terminated"));
    ASSERT_TRUE(first.IsSet());

    // A delivery report for a promise that was already failed must not resurrect or wedge anything.
    queue.Complete(1, TError());
    EXPECT_FALSE(ResultOf(first).IsOK());
    EXPECT_EQ(queue.GetFatalError().GetMessage(), "Kafka writer terminated");
}

TEST(TKafkaWriteQueueTest, ResumesFromAPersistedSeqNo)
{
    TKafkaWriteQueue queue;

    // After a restart the ordered sink base resumes numbering from the persisted seqNo, so the queue
    // must anchor on the first seqNo it sees rather than assuming it starts at one.
    auto first = queue.Enqueue(MakeMessage(4'097));
    auto second = queue.Enqueue(MakeMessage(4'098));

    queue.Complete(4'098, TError());
    EXPECT_FALSE(second.IsSet());

    queue.Complete(4'097, TError());
    EXPECT_TRUE(first.IsSet());
    EXPECT_TRUE(second.IsSet());
}

TEST(TKafkaWriteQueueTest, FailsEveryOutstandingPromise)
{
    TKafkaWriteQueue queue;

    auto first = queue.Enqueue(MakeMessage(1));
    auto second = queue.Enqueue(MakeMessage(2));
    queue.Complete(1, TError());

    queue.Fail(TError("Kafka writer terminated"));

    EXPECT_TRUE(ResultOf(first).IsOK());
    ASSERT_TRUE(second.IsSet());
    EXPECT_FALSE(ResultOf(second).IsOK());
    EXPECT_EQ(ResultOf(second).GetMessage(), "Kafka writer terminated");
    EXPECT_TRUE(queue.TakePending().empty());
}

TEST(TKafkaWriteQueueTest, FailsFastAfterAFatalError)
{
    TKafkaWriteQueue queue;

    queue.Fail(TError("Failed to create Kafka producer"));

    auto future = queue.Enqueue(MakeMessage(1));
    ASSERT_TRUE(future.IsSet());
    EXPECT_FALSE(ResultOf(future).IsOK());
    EXPECT_EQ(ResultOf(future).GetMessage(), "Failed to create Kafka producer");
    // Nothing is queued for a producer that can no longer make progress.
    EXPECT_TRUE(queue.TakePending().empty());
}

TEST(TKafkaWriteQueueTest, IgnoresACompletionForAnUnknownSeqNo)
{
    TKafkaWriteQueue queue;

    auto first = queue.Enqueue(MakeMessage(1));

    // The produce-failure path completes a seqNo the delivery report will never mention; a stray
    // completion must not wedge the prefix.
    queue.Complete(1, TError());
    queue.Complete(2, TError());

    EXPECT_TRUE(first.IsSet());

    auto third = queue.Enqueue(MakeMessage(3));
    queue.Complete(3, TError());
    EXPECT_TRUE(third.IsSet());
}

TEST(TKafkaWriteQueueTest, ResolvesAMultiRecordSeqNoOnTheLastAcknowledgement)
{
    TKafkaWriteQueue queue;

    // A split batch: three records acknowledged together under one seqNo.
    auto batch = queue.Enqueue(1, {MakeMessage(1), MakeMessage(1), MakeMessage(1)});
    auto next = queue.Enqueue(MakeMessage(2));
    EXPECT_EQ(std::ssize(queue.TakePending()), 4);

    queue.Complete(1, TError());
    queue.Complete(2, TError());
    // The next seqNo may not resolve past a partially acknowledged batch.
    EXPECT_FALSE(batch.IsSet());
    EXPECT_FALSE(next.IsSet());

    queue.Complete(1, TError());
    EXPECT_FALSE(batch.IsSet());

    queue.Complete(1, TError());
    EXPECT_TRUE(ResultOf(batch).IsOK());
    EXPECT_TRUE(ResultOf(next).IsOK());
}

TEST(TKafkaWriteQueueTest, FailsAMultiRecordSeqNoOnTheFirstFailedRecord)
{
    TKafkaWriteQueue queue;

    auto batch = queue.Enqueue(1, {MakeMessage(1), MakeMessage(1), MakeMessage(1)});

    queue.Complete(1, TError());
    queue.Complete(1, TError("Kafka delivery failed"));

    ASSERT_TRUE(batch.IsSet());
    EXPECT_FALSE(ResultOf(batch).IsOK());
    // The failure poisons the queue like any other.
    EXPECT_FALSE(queue.GetFatalError().IsOK());
}

TEST(TKafkaWriteQueueTest, KeepsTheFirstFailureOfABlockedSeqNo)
{
    TKafkaWriteQueue queue;

    auto first = queue.Enqueue(MakeMessage(1));
    auto second = queue.Enqueue(2, {MakeMessage(2), MakeMessage(2)});

    // While blocked behind seqNo 1, seqNo 2 fails one record and then reports the other OK. The
    // success must not overwrite the stored failure, or the sink base would persist past a record
    // that never reached Kafka.
    queue.Complete(2, TError("Kafka delivery failed"));
    queue.Complete(2, TError());
    queue.Complete(1, TError());

    EXPECT_TRUE(ResultOf(first).IsOK());
    ASSERT_TRUE(second.IsSet());
    EXPECT_FALSE(ResultOf(second).IsOK());
    EXPECT_FALSE(queue.GetFatalError().IsOK());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TKafkaRecordGroupingTest, ChecksTheBoundBeforeAddingARow)
{
    // Two rows that only jointly exceed the cap must land in separate records: a threshold applied
    // after the add would emit one oversized record.
    EXPECT_EQ(GroupKafkaRecordRows({600, 600}, 900), (std::vector{1, 1}));
    EXPECT_EQ(GroupKafkaRecordRows({400, 400, 400}, 900), (std::vector{2, 1}));
}

TEST(TKafkaRecordGroupingTest, FillsRecordsUpToTheBound)
{
    EXPECT_EQ(GroupKafkaRecordRows({300, 300, 300}, 900), (std::vector{3}));
    EXPECT_EQ(GroupKafkaRecordRows({1, 1, 1, 1}, 2), (std::vector{2, 2}));
    EXPECT_TRUE(GroupKafkaRecordRows({}, 900).empty());
}

TEST(TKafkaRecordGroupingTest, AnOversizedRowFormsItsOwnRecord)
{
    // A single row above the cap cannot be split; it goes alone (and fails visibly at the broker)
    // without dragging its neighbours into the oversized record.
    EXPECT_EQ(GroupKafkaRecordRows({100, 2'000, 100}, 900), (std::vector{1, 1, 1}));
    EXPECT_EQ(GroupKafkaRecordRows({2'000}, 900), (std::vector{1}));
}

TEST(TKafkaWriteQueueTest, ARejectedSeqNoAnchorsAFreshFrontier)
{
    TKafkaWriteQueue queue;

    // The first batch after writer init is rejected before anything is enqueued. It must still anchor
    // the frontier: were it skipped, the next batch would anchor past it, resolve OK, and a later
    // Sync would persist the rejected messages as written — the replay then deduplicates them away.
    queue.Reject(1, TError("Failed to pack Kafka batch"));

    EXPECT_FALSE(queue.GetFatalError().IsOK());
    auto second = queue.Enqueue(MakeMessage(2));
    ASSERT_TRUE(second.IsSet());
    EXPECT_FALSE(ResultOf(second).IsOK());
    EXPECT_TRUE(queue.TakePending().empty());
}

TEST(TKafkaWriteQueueTest, ARejectedSeqNoPoisonsAnEstablishedFrontierInOrder)
{
    TKafkaWriteQueue queue;

    auto first = queue.Enqueue(MakeMessage(1));
    queue.Reject(2, TError("Failed to pack Kafka batch"));
    auto third = queue.Enqueue(MakeMessage(3));

    // The write before the rejection still resolves truthfully; everything after fails instead of
    // wedging forever behind the gap the rejected seqNo would otherwise have left.
    queue.Complete(1, TError());
    EXPECT_TRUE(ResultOf(first).IsOK());
    ASSERT_TRUE(third.IsSet());
    EXPECT_FALSE(ResultOf(third).IsOK());
    EXPECT_FALSE(queue.GetFatalError().IsOK());
}

TEST(TKafkaWriteQueueTest, CarriesTheMessageIdHeaderValue)
{
    TKafkaWriteQueue queue;

    auto message = MakeMessage(1);
    message.MessageId = "stream:0:42";
    message.Key = "key";
    Y_UNUSED(queue.Enqueue(std::move(message)));

    auto pending = queue.TakePending();
    ASSERT_EQ(std::ssize(pending), 1);
    ASSERT_TRUE(pending[0].MessageId.has_value());
    EXPECT_EQ(*pending[0].MessageId, "stream:0:42");
    ASSERT_TRUE(pending[0].Key.has_value());
    EXPECT_EQ(*pending[0].Key, "key");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
