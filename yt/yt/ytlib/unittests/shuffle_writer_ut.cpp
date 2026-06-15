#include <yt/yt/ytlib/push_based_shuffle_client/config.h>
#include <yt/yt/ytlib/push_based_shuffle_client/session_provider.h>
#include <yt/yt/ytlib/push_based_shuffle_client/shuffle_writer.h>

#include <yt/yt/ytlib/distributed_chunk_session_client/distributed_chunk_session_pool.h>
#include <yt/yt/ytlib/distributed_chunk_session_client/distributed_chunk_writer.h>

#include <yt/yt/ytlib/chunk_client/session_id.h>

#include <yt/yt/ytlib/table_client/partitioner.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/actions/callback.h>
#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/concurrency/suspendable_action_queue.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <deque>

namespace NYT::NPushBasedShuffleClient {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NDistributedChunkSessionClient;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

// Test-only factory: takes an injectable callback for constructing
// IDistributedChunkWriter so tests can substitute a fake. Defined in
// yt/yt/ytlib/push_based_shuffle_client/shuffle_writer.cpp.
using TCreateDistributedChunkWriterCallback = TCallback<
    IDistributedChunkWriterPtr(TNodeDescriptor, TSessionId)>;

IPushBasedShuffleWriterPtr CreatePushBasedShuffleWriterForTesting(
    TShuffleWriterConfigPtr config,
    IPartitionWriteSessionProviderPtr sessionProvider,
    IPartitionerPtr partitioner,
    TCreateDistributedChunkWriterCallback createDistributedChunkWriter,
    i32 mapperId,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

TSessionDescriptor MakeSessionDescriptor(ui64 counter, const std::string& address)
{
    auto chunkId = MakeId(EObjectType::JournalChunk, TCellTag(1), counter, 0);
    return TSessionDescriptor{
        .SessionId = TSessionId(chunkId, /*mediumIndex*/ 0),
        .SequencerNode = TNodeDescriptor(address),
    };
}

////////////////////////////////////////////////////////////////////////////////

// State is touched by GetSession (invoker thread) and ProvideSession /
// ProvideError / GetCalls (main test thread). A spinlock guards mutations.
class TFakeProvider
    : public IPartitionWriteSessionProvider
{
public:
    TFuture<TSessionDescriptor> GetSession(
        int partitionIndex,
        std::optional<TSessionId> excludedSessionId) override
    {
        auto guard = Guard(Lock_);
        Calls_.push_back({partitionIndex, excludedSessionId});
        auto promise = NewPromise<TSessionDescriptor>();
        Pending_[partitionIndex].push_back(promise);
        TryFulfillUnderLock(partitionIndex);
        return promise.ToFuture();
    }

    //! Queues a successful response for partitionIndex. Fulfils any pending
    //! GetSession future first; otherwise stays queued for the next call.
    void ProvideSession(int partitionIndex, TSessionDescriptor session)
    {
        auto guard = Guard(Lock_);
        Responses_[partitionIndex].push_back(std::move(session));
        TryFulfillUnderLock(partitionIndex);
    }

    //! Queues an error response for partitionIndex.
    void ProvideError(int partitionIndex, TError error)
    {
        auto guard = Guard(Lock_);
        ErrorResponses_[partitionIndex].push_back(std::move(error));
        TryFulfillUnderLock(partitionIndex);
    }

    std::vector<std::pair<int, std::optional<TSessionId>>> GetCalls() const
    {
        auto guard = Guard(Lock_);
        return Calls_;
    }

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    THashMap<int, std::deque<TPromise<TSessionDescriptor>>> Pending_;
    THashMap<int, std::deque<TSessionDescriptor>> Responses_;
    THashMap<int, std::deque<TError>> ErrorResponses_;
    std::vector<std::pair<int, std::optional<TSessionId>>> Calls_;

    void TryFulfillUnderLock(int partitionIndex)
    {
        YT_ASSERT_SPINLOCK_AFFINITY(Lock_);
        auto& pending = Pending_[partitionIndex];
        auto& responses = Responses_[partitionIndex];
        auto& errorResponses = ErrorResponses_[partitionIndex];

        while (!pending.empty() && (!responses.empty() || !errorResponses.empty())) {
            auto promise = std::move(pending.front());
            pending.pop_front();

            if (!errorResponses.empty()) {
                auto error = std::move(errorResponses.front());
                errorResponses.pop_front();
                // promise.Set fires Subscribe(...).Via(invoker) callbacks by
                // posting to the invoker; no synchronous callback runs.
                promise.Set(std::move(error));
            } else {
                auto session = std::move(responses.front());
                responses.pop_front();
                promise.Set(std::move(session));
            }
        }
    }
};

using TFakeProviderPtr = TIntrusivePtr<TFakeProvider>;

////////////////////////////////////////////////////////////////////////////////

// State is touched by WriteRecord (invoker thread) and Ack / accessors
// (main test thread). A spinlock guards mutations.
class TFakeDistributedChunkWriter
    : public IDistributedChunkWriter
{
public:
    explicit TFakeDistributedChunkWriter(TSessionId sessionId)
        : SessionId_(sessionId)
    { }

    TFuture<void> WriteRecord(TSharedRef record) override
    {
        auto guard = Guard(Lock_);
        Records_.push_back(std::move(record));
        auto promise = NewPromise<void>();
        Promises_.push_back(promise);
        TryFulfillUnderLock();
        return promise.ToFuture();
    }

    //! Queues an ack response (success by default; pass error for failure).
    //! Fulfils the next not-yet-set promise if one exists; otherwise queues.
    void Ack(TError error = {})
    {
        auto guard = Guard(Lock_);
        Responses_.push_back(std::move(error));
        TryFulfillUnderLock();
    }

    TSessionId GetSessionId() const
    {
        return SessionId_;
    }

    std::vector<TSharedRef> GetRecords() const
    {
        auto guard = Guard(Lock_);
        return Records_;
    }

    int GetUnackedCount() const
    {
        auto guard = Guard(Lock_);
        int count = 0;
        for (const auto& promise : Promises_) {
            if (!promise.IsSet()) {
                ++count;
            }
        }
        return count;
    }

private:
    const TSessionId SessionId_;
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    std::vector<TSharedRef> Records_;
    std::vector<TPromise<void>> Promises_;
    std::deque<TError> Responses_;

    void TryFulfillUnderLock()
    {
        YT_ASSERT_SPINLOCK_AFFINITY(Lock_);
        for (i64 index = 0; index < std::ssize(Promises_) && !Responses_.empty(); ++index) {
            if (!Promises_[index].IsSet()) {
                Promises_[index].Set(std::move(Responses_.front()));
                Responses_.pop_front();
            }
        }
    }
};

using TFakeDistributedChunkWriterPtr = TIntrusivePtr<TFakeDistributedChunkWriter>;

////////////////////////////////////////////////////////////////////////////////

class TWriterHarness
{
public:
    TWriterHarness()
        : ActionQueue_(CreateSuspendableActionQueue("ShuffleWriterTest"))
        , Provider_(New<TFakeProvider>())
        , RowBuffer_(New<TRowBuffer>())
    { }

    IPushBasedShuffleWriterPtr CreateWriter(
        int partitionCount,
        i64 memoryBudget = 1_MB,
        i32 mapperId = 17,
        std::optional<double> buildersBudgetFraction = std::nullopt)
    {
        auto config = New<TShuffleWriterConfig>();
        config->SetDefaults();
        config->MemoryBudget = memoryBudget;
        if (buildersBudgetFraction) {
            config->BuildersBudgetFraction = *buildersBudgetFraction;
        }

        auto partitioner = CreateColumnBasedPartitioner(partitionCount, /*partitionColumnId*/ 0);

        auto createDistributedChunkWriter = BIND([this] (TNodeDescriptor /*node*/, TSessionId sessionId) {
            auto writer = New<TFakeDistributedChunkWriter>(sessionId);
            {
                auto guard = Guard(ChunkWritersLock_);
                ChunkWriters_[sessionId] = writer;
            }
            return IDistributedChunkWriterPtr(writer);
        });

        return CreatePushBasedShuffleWriterForTesting(
            std::move(config),
            Provider_,
            std::move(partitioner),
            std::move(createDistributedChunkWriter),
            mapperId,
            ActionQueue_->GetInvoker());
    }

    void DrainInvoker()
    {
        WaitFor(ActionQueue_->Suspend(/*immediately*/ false))
            .ThrowOnError();
        ActionQueue_->Resume();
    }

    TFakeProviderPtr GetProvider() const { return Provider_; }

    //! Snapshot of currently known fake chunk writers, indexed by session id.
    //! ChunkWriters_ is mutated on the invoker thread (when a session is
    //! resolved) and read on the test thread; the spinlock makes a safe copy.
    THashMap<TSessionId, TFakeDistributedChunkWriterPtr> GetChunkWriters() const
    {
        auto guard = Guard(ChunkWritersLock_);
        return ChunkWriters_;
    }

    //! Construct a row whose partition_column (column id 0) is partitionIndex
    //! and which carries a single i64 payload column with the given value.
    TUnversionedRow MakeRow(int partitionIndex, i64 payload)
    {
        // `builder` must outlive the CaptureRow call: `builder.GetRow()`
        // returns a non-owning TUnversionedRow that points into builder's
        // storage. Splitting builder construction into a separate function
        // would let the storage die before CaptureRow reads it.
        TUnversionedRowBuilder builder;
        builder.AddValue(MakeUnversionedInt64Value(partitionIndex, /*id*/ 0));
        builder.AddValue(MakeUnversionedInt64Value(payload, /*id*/ 1));
        return RowBuffer_->CaptureRow(builder.GetRow());
    }

private:
    const ISuspendableActionQueuePtr ActionQueue_;
    const TFakeProviderPtr Provider_;
    const TRowBufferPtr RowBuffer_;
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, ChunkWritersLock_);
    THashMap<TSessionId, TFakeDistributedChunkWriterPtr> ChunkWriters_;
};

////////////////////////////////////////////////////////////////////////////////

TEST(TPushBasedShuffleWriterTest, EmptyCloseSucceedsImmediately)
{
    TWriterHarness h;
    auto writer = h.CreateWriter(/*partitionCount*/ 1);
    auto closeFuture = writer->Close();
    h.DrainInvoker();
    EXPECT_TRUE(closeFuture.IsSet());
    EXPECT_TRUE(closeFuture.GetOrCrash().IsOK());
    EXPECT_TRUE(h.GetChunkWriters().empty());
}

TEST(TPushBasedShuffleWriterTest, SingleRowSinglePartitionFlushesOnClose)
{
    TWriterHarness h;
    auto writer = h.CreateWriter(/*partitionCount*/ 1);

    std::vector<TUnversionedRow> rows = {h.MakeRow(/*partitionIndex*/ 0, /*payload*/ 42)};
    auto writeFuture = writer->Write(TRange(rows));
    h.DrainInvoker();
    ASSERT_TRUE(writeFuture.IsSet());
    EXPECT_TRUE(writeFuture.GetOrCrash().IsOK());

    auto session = MakeSessionDescriptor(/*counter*/ 1, "node-1");
    h.GetProvider()->ProvideSession(/*partitionIndex*/ 0, session);
    h.DrainInvoker();

    ASSERT_EQ(h.GetChunkWriters().size(), 1u);
    auto chunkWriter = h.GetChunkWriters().begin()->second;
    EXPECT_EQ(chunkWriter->GetUnackedCount(), 0);   // no record yet — flush happens in Close

    auto closeFuture = writer->Close();
    h.DrainInvoker();

    EXPECT_EQ(std::ssize(chunkWriter->GetRecords()), 1);
    EXPECT_EQ(chunkWriter->GetUnackedCount(), 1);

    chunkWriter->Ack();
    h.DrainInvoker();
    EXPECT_TRUE(closeFuture.IsSet());
    EXPECT_TRUE(closeFuture.GetOrCrash().IsOK());
}

TEST(TPushBasedShuffleWriterTest, RowsRoutedAcrossPartitions)
{
    constexpr int PartitionCount = 4;
    TWriterHarness h;
    auto writer = h.CreateWriter(PartitionCount);

    std::vector<TUnversionedRow> rows;
    for (int partitionIndex = 0; partitionIndex < PartitionCount; ++partitionIndex) {
        rows.push_back(h.MakeRow(/*partitionIndex*/ partitionIndex, /*payload*/ 100 + partitionIndex));
        rows.push_back(h.MakeRow(/*partitionIndex*/ partitionIndex, /*payload*/ 200 + partitionIndex));
    }
    WaitFor(writer->Write(TRange(rows))).ThrowOnError();
    h.DrainInvoker();

    for (int partitionIndex = 0; partitionIndex < PartitionCount; ++partitionIndex) {
        h.GetProvider()->ProvideSession(partitionIndex, MakeSessionDescriptor(partitionIndex + 1, Format("node-%v", partitionIndex)));
    }
    h.DrainInvoker();

    auto closeFuture = writer->Close();
    h.DrainInvoker();

    EXPECT_EQ(h.GetChunkWriters().size(), static_cast<size_t>(PartitionCount));
    for (const auto& [sessionId, chunkWriter] : h.GetChunkWriters()) {
        EXPECT_EQ(std::ssize(chunkWriter->GetRecords()), 1)
            << "session " << ToString(sessionId) << " expected one flushed record";
    }

    for (const auto& [sessionId, chunkWriter] : h.GetChunkWriters()) {
        chunkWriter->Ack();
    }
    h.DrainInvoker();
    EXPECT_TRUE(closeFuture.IsSet());
    EXPECT_TRUE(closeFuture.GetOrCrash().IsOK());
}

TEST(TPushBasedShuffleWriterTest, BudgetTriggersEvictionMidBatch)
{
    constexpr int PartitionCount = 2;
    // Tight budget: 256 KiB total → 204 KiB builders. Adding enough rows to
    // partition 0 pushes BuildersBytes past BuildersBudget and triggers
    // eviction of partition 0's builder. A second Write adds more rows to
    // partition 0 so that Close flushes a second record for it, giving
    // totalRecords > PartitionCount.
    TWriterHarness h;
    auto writer = h.CreateWriter(PartitionCount, /*memoryBudget*/ 256_KB);

    // First batch: many rows to partition 0 (triggers eviction) + one to
    // partition 1. The evicted record exceeds InFlightBudget, so the write
    // future may be held under backpressure — don't block on it yet.
    std::vector<TUnversionedRow> batch0;
    for (int rowIndex = 0; rowIndex < 50000; ++rowIndex) {
        batch0.push_back(h.MakeRow(0, rowIndex));
    }
    batch0.push_back(h.MakeRow(1, 1));
    auto writeFuture0 = writer->Write(TRange(batch0));
    h.DrainInvoker();

    // Eviction of partition 0 should have triggered a session request.
    auto calls = h.GetProvider()->GetCalls();
    bool sawPartitionZero = false;
    for (const auto& [callPartition, excl] : calls) {
        if (callPartition == 0) sawPartitionZero = true;
    }
    EXPECT_TRUE(sawPartitionZero) << "expected partition 0 to request a session after eviction";

    for (int partitionIndex = 0; partitionIndex < PartitionCount; ++partitionIndex) {
        h.GetProvider()->ProvideSession(partitionIndex, MakeSessionDescriptor(partitionIndex + 1, Format("node-%v", partitionIndex)));
    }
    h.DrainInvoker();

    // Sessions resolved and DrainPending shipped the evicted records to the
    // fakes, but InFlightBytes is still above InFlightBudget (no acks yet),
    // so the write future must still be held by backpressure.
    EXPECT_FALSE(writeFuture0.IsSet())
        << "expected backpressure to hold the write future until acks drain";

    // Drain backpressure from the first write by acking in-flight records.
    for (const auto& [sessionId, chunkWriter] : h.GetChunkWriters()) {
        while (chunkWriter->GetUnackedCount() > 0) {
            chunkWriter->Ack();
            h.DrainInvoker();
        }
    }
    ASSERT_TRUE(writeFuture0.IsSet());
    ASSERT_TRUE(writeFuture0.GetOrCrash().IsOK());

    // Second batch: a few more rows to partition 0 so Close has something to flush.
    std::vector<TUnversionedRow> batch1;
    for (int rowIndex = 0; rowIndex < 5; ++rowIndex) {
        batch1.push_back(h.MakeRow(0, 100000 + rowIndex));
    }
    WaitFor(writer->Write(TRange(batch1))).ThrowOnError();
    h.DrainInvoker();

    auto closeFuture = writer->Close();
    h.DrainInvoker();
    for (const auto& [sessionId, chunkWriter] : h.GetChunkWriters()) {
        while (chunkWriter->GetUnackedCount() > 0) {
            chunkWriter->Ack();
            h.DrainInvoker();
        }
    }
    EXPECT_TRUE(closeFuture.GetOrCrash().IsOK());

    // With eviction in play, partition 0 should produce two records:
    // the evicted one (from the first Write) and the close flush.
    int totalRecords = 0;
    for (const auto& [sessionId, chunkWriter] : h.GetChunkWriters()) {
        totalRecords += std::ssize(chunkWriter->GetRecords());
    }
    EXPECT_GT(totalRecords, PartitionCount)
        << "expected eviction to produce more records than one-per-partition";
}

TEST(TPushBasedShuffleWriterTest, WriteFutureDeferredWhenInFlightBudgetFull)
{
    constexpr int PartitionCount = 1;
    // BuildersFraction=0.8 → InFlightBudget = MemoryBudget * 0.2.
    // With a tight memory budget and lots of rows, the flush(es) will fill
    // InFlightBytes beyond InFlightBudget and trigger backpressure.
    TWriterHarness h;
    auto writer = h.CreateWriter(
        PartitionCount,
        /*memoryBudget*/ 128_KB,
        /*mapperId*/ 17,
        /*buildersBudgetFraction*/ 0.8);

    std::vector<TUnversionedRow> rows;
    for (int rowIndex = 0; rowIndex < 50000; ++rowIndex) {
        rows.push_back(h.MakeRow(0, rowIndex));
    }
    auto writeFuture = writer->Write(TRange(rows));
    h.DrainInvoker();

    h.GetProvider()->ProvideSession(0, MakeSessionDescriptor(1, "node-0"));
    h.DrainInvoker();

    // Write's future should not be set yet because InFlight is over cap.
    EXPECT_FALSE(writeFuture.IsSet())
        << "expected backpressure to hold the write future";

    // Ack all in-flight records.
    auto chunkWriter = h.GetChunkWriters().begin()->second;
    while (chunkWriter->GetUnackedCount() > 0) {
        chunkWriter->Ack();
        h.DrainInvoker();
    }

    EXPECT_TRUE(writeFuture.IsSet());
    EXPECT_TRUE(writeFuture.GetOrCrash().IsOK());

    // Clean up.
    auto closeFuture = writer->Close();
    h.DrainInvoker();
    while (chunkWriter->GetUnackedCount() > 0) {
        chunkWriter->Ack();
        h.DrainInvoker();
    }
    EXPECT_TRUE(closeFuture.GetOrCrash().IsOK());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TPushBasedShuffleWriterTest, ResendOnNewSessionAfterWriteRecordFailure)
{
    constexpr int PartitionCount = 1;
    TWriterHarness h;
    auto writer = h.CreateWriter(PartitionCount);

    std::vector<TUnversionedRow> rows = {h.MakeRow(0, 42)};
    WaitFor(writer->Write(TRange(rows))).ThrowOnError();
    h.DrainInvoker();

    auto session1 = MakeSessionDescriptor(/*counter*/ 1, "node-1");
    h.GetProvider()->ProvideSession(0, session1);
    h.DrainInvoker();

    auto closeFuture = writer->Close();
    h.DrainInvoker();

    // The record has been sent on session 1 but not yet acked.
    auto writer1 = h.GetChunkWriters().at(session1.SessionId);
    ASSERT_EQ(std::ssize(writer1->GetRecords()), 1);
    ASSERT_EQ(writer1->GetUnackedCount(), 1);

    // Fail the first ack. The writer should request a new session, excluding session1.
    writer1->Ack(TError("simulated write failure"));
    h.DrainInvoker();

    bool sawExclusion = false;
    for (const auto& [callPartition, excl] : h.GetProvider()->GetCalls()) {
        if (excl && *excl == session1.SessionId) {
            sawExclusion = true;
            break;
        }
    }
    EXPECT_TRUE(sawExclusion) << "expected GetSession(0, excluded=session1)";

    auto session2 = MakeSessionDescriptor(/*counter*/ 2, "node-2");
    h.GetProvider()->ProvideSession(0, session2);
    h.DrainInvoker();

    auto writer2 = h.GetChunkWriters().at(session2.SessionId);
    EXPECT_EQ(std::ssize(writer2->GetRecords()), 1) << "record should be resent on session 2";

    writer2->Ack();
    h.DrainInvoker();
    EXPECT_TRUE(closeFuture.IsSet());
    EXPECT_TRUE(closeFuture.GetOrCrash().IsOK());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TPushBasedShuffleWriterTest, SweepInFlightAtSessionResolved)
{
    constexpr int PartitionCount = 1;
    TWriterHarness h;
    // Budget: 200 KB total, builderFraction=0.4 → builder budget=80 KB,
    // in-flight budget=120 KB.  5 000 rows at ~17 bytes/row (GetCapacity) ≈ 84 KB
    // which exceeds the 80 KB builder budget, so each Write batch independently
    // triggers one eviction.  Each evicted record is ~80 KB which fits inside
    // the 120 KB in-flight budget, so there is no backpressure after the first
    // eviction and the second Write can proceed immediately.
    auto writer = h.CreateWriter(
        PartitionCount,
        /*memoryBudget*/ 200_KB,
        /*mapperId*/ 17,
        /*buildersBudgetFraction*/ 0.4);

    // 5 000 rows at ~17 bytes/row (GetCapacity) ≈ 84 KB > 80 KB builder budget →
    // each batch independently triggers one eviction, producing one record per batch.
    std::vector<TUnversionedRow> batch0;
    for (int rowIndex = 0; rowIndex < 5000; ++rowIndex) {
        batch0.push_back(h.MakeRow(0, rowIndex));
    }
    WaitFor(writer->Write(TRange(batch0))).ThrowOnError();
    h.DrainInvoker();

    std::vector<TUnversionedRow> batch1;
    for (int rowIndex = 0; rowIndex < 5000; ++rowIndex) {
        batch1.push_back(h.MakeRow(0, 5000 + rowIndex));
    }
    WaitFor(writer->Write(TRange(batch1))).ThrowOnError();
    h.DrainInvoker();

    auto session1 = MakeSessionDescriptor(1, "node-1");
    h.GetProvider()->ProvideSession(0, session1);
    h.DrainInvoker();

    auto writer1 = h.GetChunkWriters().at(session1.SessionId);
    int recordsOnSession1 = std::ssize(writer1->GetRecords());
    ASSERT_GT(recordsOnSession1, 1) << "expected more than one record on session 1";

    // Fail the first ack — retire session1, request session2.
    writer1->Ack(TError("simulated"));
    h.DrainInvoker();

    auto session2 = MakeSessionDescriptor(2, "node-2");
    h.GetProvider()->ProvideSession(0, session2);
    h.DrainInvoker();

    // After session2 resolves, all still-in-flight records from session1
    // (i.e. recordsOnSession1 - 1) plus any queued Pending should be sent
    // on session2.
    auto writer2 = h.GetChunkWriters().at(session2.SessionId);
    EXPECT_EQ(std::ssize(writer2->GetRecords()), recordsOnSession1)
        << "expected the failed record + all swept in-flight records on session 2";

    while (writer2->GetUnackedCount() > 0) {
        writer2->Ack();
        h.DrainInvoker();
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TPushBasedShuffleWriterTest, MaxSendAttemptsExceededFailsWriter)
{
    constexpr int PartitionCount = 1;
    TWriterHarness h;
    auto writer = h.CreateWriter(PartitionCount);

    std::vector<TUnversionedRow> rows = {h.MakeRow(0, 42)};
    WaitFor(writer->Write(TRange(rows))).ThrowOnError();
    h.DrainInvoker();

    auto closeFuture = writer->Close();
    h.DrainInvoker();

    // Default MaxSendAttempts is 3 (from TShuffleWriterConfig::Register).
    // Provide three sessions; each WriteRecord fails.
    for (int attempt = 1; attempt <= 3; ++attempt) {
        auto session = MakeSessionDescriptor(attempt, Format("node-%v", attempt));
        h.GetProvider()->ProvideSession(0, session);
        h.DrainInvoker();
        auto writer1 = h.GetChunkWriters().at(session.SessionId);
        writer1->Ack(TError("simulated failure"));
        h.DrainInvoker();
    }

    ASSERT_TRUE(closeFuture.IsSet());
    EXPECT_FALSE(closeFuture.GetOrCrash().IsOK());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TPushBasedShuffleWriterTest, GetSessionFailureFailsWriter)
{
    constexpr int PartitionCount = 1;
    TWriterHarness h;
    auto writer = h.CreateWriter(PartitionCount);

    std::vector<TUnversionedRow> rows = {h.MakeRow(0, 42)};
    WaitFor(writer->Write(TRange(rows))).ThrowOnError();
    h.DrainInvoker();

    auto closeFuture = writer->Close();
    h.DrainInvoker();

    h.GetProvider()->ProvideError(0, TError("session creation failed"));
    h.DrainInvoker();

    ASSERT_TRUE(closeFuture.IsSet());
    EXPECT_FALSE(closeFuture.GetOrCrash().IsOK());
}

TEST(TPushBasedShuffleWriterTest, CloseIsIdempotent)
{
    TWriterHarness h;
    auto writer = h.CreateWriter(/*partitionCount*/ 1);
    auto close1 = writer->Close();
    auto close2 = writer->Close();
    h.DrainInvoker();
    EXPECT_TRUE(close1.IsSet());
    EXPECT_TRUE(close2.IsSet());
    EXPECT_TRUE(close1.GetOrCrash().IsOK());
    EXPECT_TRUE(close2.GetOrCrash().IsOK());
}

////////////////////////////////////////////////////////////////////////////////

// Exercises the MaxSendAttempts cap with two records cycling through retries.
// Two records are evicted to Pending before any session is provided. Each
// iteration provides a session: DrainPending sends both, then one ack fails,
// which retires the session and sweeps the surviving record back to Pending
// on the next OnSessionResolved. After MaxSendAttempts iterations a failing
// ack trips the cap in OnWriteResponse and FailWriter fires.
//
// Note: DrainPending also has a cap check, but reaching it requires an
// asymmetric in-flight scenario (one record at SendAttempts == MaxSendAttempts
// swept back to Pending before its own ack fires) that the FIFO fake makes
// hard to construct deterministically. The DrainPending check stays in the
// code as defensive but is not exercised here.
TEST(TPushBasedShuffleWriterTest, MaxSendAttemptsExhaustedWithMultipleRecords)
{
    constexpr int PartitionCount = 1;
    TWriterHarness h;
    // Budget: 200 KB total, builderFraction=0.4 → BuildersBudget=80 KB,
    // InFlightBudget=120 KB. ~5000 rows × 17 bytes ≈ 84 KB > 80 KB →
    // each Write batch independently triggers eviction.
    // Two Write calls produce two compressed records in Pending/InFlight whose
    // combined compressed size fits inside InFlightBudget (no backpressure).
    auto writer = h.CreateWriter(
        PartitionCount,
        /*memoryBudget*/ 200_KB,
        /*mapperId*/ 17,
        /*buildersBudgetFraction*/ 0.4);

    // First batch — triggers eviction, record A goes to Pending.
    std::vector<TUnversionedRow> batch0;
    for (int rowIndex = 0; rowIndex < 5000; ++rowIndex) {
        batch0.push_back(h.MakeRow(0, rowIndex));
    }
    WaitFor(writer->Write(TRange(batch0))).ThrowOnError();
    h.DrainInvoker();

    // Second batch — triggers another eviction, record B goes to Pending.
    std::vector<TUnversionedRow> batch1;
    for (int rowIndex = 0; rowIndex < 5000; ++rowIndex) {
        batch1.push_back(h.MakeRow(0, 5000 + rowIndex));
    }
    WaitFor(writer->Write(TRange(batch1))).ThrowOnError();
    h.DrainInvoker();

    auto closeFuture = writer->Close();
    h.DrainInvoker();

    // Drive a sequence of session resolves. On each iteration:
    //   1. Session N is provided; DrainPending sends all Pending records
    //      (A and B), incrementing their SendAttempts.
    //   2. We fail the ack for one record. OnWriteResponse retires session N and
    //      re-queues that record. Any remaining InFlight records from
    //      session N are swept back to Pending when session N+1 resolves.
    //   3. After MaxSendAttempts iterations, DrainPending detects that a
    //      record's SendAttempts >= MaxSendAttempts and fires FailWriter.
    for (int attempt = 1; attempt <= 5; ++attempt) {
        auto session = MakeSessionDescriptor(attempt, Format("node-%v", attempt));
        h.GetProvider()->ProvideSession(0, session);
        h.DrainInvoker();
        auto writers = h.GetChunkWriters();
        auto sessionIt = writers.find(session.SessionId);
        if (sessionIt == writers.end()) {
            break;
        }
        auto chunkWriter = sessionIt->second;
        if (attempt == 1) {
            // Verify the setup actually exercised eviction: both batches must
            // have produced an evicted record so DrainPending sees 2 entries.
            // If THorizontalBlockWriter allocation patterns change such that
            // 5000 rows no longer exceed the 80 KB builder budget, recalibrate
            // the batch size — otherwise this test silently degrades into the
            // single-record-on-OnWriteResponse path.
            ASSERT_GE(chunkWriter->GetUnackedCount(), 2)
                << "expected eviction to produce two in-flight records on the first session";
        }
        if (chunkWriter->GetUnackedCount() == 0) {
            break;
        }
        // Fail the ack for one record on this session.
        chunkWriter->Ack(TError("simulated failure"));
        h.DrainInvoker();
    }

    ASSERT_TRUE(closeFuture.IsSet());
    EXPECT_FALSE(closeFuture.GetOrCrash().IsOK());

    // After FailWriter fires, the writer is terminal but the chunk writers
    // from previous attempts still hold unset promises. Drain them so the
    // subscribed OnWriteResponse callbacks fire and drop their MakeStrong(this) refs;
    // otherwise the writer leaks through pending subscribers.
    for (const auto& [sessionId, chunkWriter] : h.GetChunkWriters()) {
        while (chunkWriter->GetUnackedCount() > 0) {
            chunkWriter->Ack();
            h.DrainInvoker();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

// Verifies that an ack arriving for a cookie that was already swept by
// OnSessionResolved is a no-op (no double-free of InFlightBytes, no crash).
TEST(TPushBasedShuffleWriterTest, StaleAckAfterSweepIsNoOp)
{
    constexpr int PartitionCount = 1;
    TWriterHarness h;
    // Budget: 200 KB total, builderFraction=0.4 → BuildersBudget=80 KB,
    // InFlightBudget=120 KB. ~5000 rows × 17 bytes ≈ 84 KB > 80 KB →
    // each Write batch independently evicts one record. Two evictions produce
    // two in-flight records on session1 (combined compressed size fits
    // InFlightBudget, so no backpressure deadlock on Write).
    auto writer = h.CreateWriter(
        PartitionCount,
        /*memoryBudget*/ 200_KB,
        /*mapperId*/ 17,
        /*buildersBudgetFraction*/ 0.4);

    std::vector<TUnversionedRow> batch0;
    for (int rowIndex = 0; rowIndex < 5000; ++rowIndex) {
        batch0.push_back(h.MakeRow(0, rowIndex));
    }
    WaitFor(writer->Write(TRange(batch0))).ThrowOnError();
    h.DrainInvoker();

    std::vector<TUnversionedRow> batch1;
    for (int rowIndex = 0; rowIndex < 5000; ++rowIndex) {
        batch1.push_back(h.MakeRow(0, 5000 + rowIndex));
    }
    WaitFor(writer->Write(TRange(batch1))).ThrowOnError();
    h.DrainInvoker();

    auto session1 = MakeSessionDescriptor(1, "node-1");
    h.GetProvider()->ProvideSession(0, session1);
    h.DrainInvoker();

    auto writer1 = h.GetChunkWriters().at(session1.SessionId);
    int recordsOnSession1 = std::ssize(writer1->GetRecords());
    ASSERT_GT(recordsOnSession1, 1);

    // Fail the first ack — retires session1, requests session2.
    writer1->Ack(TError("simulated"));
    h.DrainInvoker();

    auto session2 = MakeSessionDescriptor(2, "node-2");
    h.GetProvider()->ProvideSession(0, session2);
    h.DrainInvoker();

    // After session2 resolves, the remaining session1 in-flight cookies have
    // been swept into Pending and then re-sent on session2. Any subsequent
    // acks on writer1 are stale and must no-op.
    auto writer2 = h.GetChunkWriters().at(session2.SessionId);
    EXPECT_EQ(std::ssize(writer2->GetRecords()), recordsOnSession1);

    // Ack the leftover session1 in-flight ones — they are stale.
    while (writer1->GetUnackedCount() > 0) {
        writer1->Ack();
        h.DrainInvoker();
    }

    // Drive Close, then ack everything on session 2 — Close may flush a final
    // record from any builder leftover beyond the mid-batch evictions, which
    // also lands on session 2.
    auto closeFuture = writer->Close();
    h.DrainInvoker();
    while (writer2->GetUnackedCount() > 0) {
        writer2->Ack();
        h.DrainInvoker();
    }
    EXPECT_TRUE(closeFuture.IsSet());
    EXPECT_TRUE(closeFuture.GetOrCrash().IsOK());
}

////////////////////////////////////////////////////////////////////////////////

// Two records are in-flight on session 1. Both acks fail. OnWriteResponse for the first
// retires session 1 and requests session 2. OnWriteResponse for the second sees
// `partitionState.Session` already nulled and must skip the retire — the guard
// `Session->SessionId == inFlight.SessionId` is what prevents requesting an
// extra excluded-session twice.
TEST(TPushBasedShuffleWriterTest, ConcurrentFailuresRetireSessionOnce)
{
    constexpr int PartitionCount = 1;
    TWriterHarness h;
    // Two batches → two evictions → two records in Pending.
    auto writer = h.CreateWriter(
        PartitionCount,
        /*memoryBudget*/ 200_KB,
        /*mapperId*/ 17,
        /*buildersBudgetFraction*/ 0.4);

    std::vector<TUnversionedRow> batch0;
    for (int rowIndex = 0; rowIndex < 5000; ++rowIndex) {
        batch0.push_back(h.MakeRow(0, rowIndex));
    }
    WaitFor(writer->Write(TRange(batch0))).ThrowOnError();
    h.DrainInvoker();

    std::vector<TUnversionedRow> batch1;
    for (int rowIndex = 0; rowIndex < 5000; ++rowIndex) {
        batch1.push_back(h.MakeRow(0, 5000 + rowIndex));
    }
    WaitFor(writer->Write(TRange(batch1))).ThrowOnError();
    h.DrainInvoker();

    auto session1 = MakeSessionDescriptor(1, "node-1");
    h.GetProvider()->ProvideSession(0, session1);
    h.DrainInvoker();

    auto writer1 = h.GetChunkWriters().at(session1.SessionId);
    ASSERT_GE(writer1->GetUnackedCount(), 2);

    // Queue both error responses before draining the invoker. The fake's
    // FIFO matching attaches each error to the corresponding in-flight
    // promise; the OnWriteResponse callbacks then fire sequentially on the invoker.
    writer1->Ack(TError("simulated 1"));
    writer1->Ack(TError("simulated 2"));
    h.DrainInvoker();

    int retireCount = 0;
    for (const auto& [callPartition, excl] : h.GetProvider()->GetCalls()) {
        if (excl && *excl == session1.SessionId) {
            ++retireCount;
        }
    }
    EXPECT_EQ(retireCount, 1)
        << "the retire guard should ensure only one excluded-session request per dying session";

    // Drain the remaining writer1 promises (stale; OnWriteResponse no-ops for them) so
    // the subscribed callbacks fire and drop their MakeStrong(this) refs.
    // Without this the writer leaks through pending subscriber callbacks.
    while (writer1->GetUnackedCount() > 0) {
        writer1->Ack();
        h.DrainInvoker();
    }

    // Cleanup so the writer can close cleanly.
    auto session2 = MakeSessionDescriptor(2, "node-2");
    h.GetProvider()->ProvideSession(0, session2);
    h.DrainInvoker();
    auto writer2 = h.GetChunkWriters().at(session2.SessionId);
    auto closeFuture = writer->Close();
    h.DrainInvoker();
    while (writer2->GetUnackedCount() > 0) {
        writer2->Ack();
        h.DrainInvoker();
    }
    EXPECT_TRUE(closeFuture.GetOrCrash().IsOK());
}

////////////////////////////////////////////////////////////////////////////////

// A and B are in-flight on session 1. A's ack fails → session 1 retired,
// session 2 requested. Before session 2 resolves, B's ack succeeds. OnWriteResponse for
// B removes the in-flight entry on the success path. When session 2 resolves,
// the sweep finds no in-flight record for the dying session 1 (B was already
// cleaned up), so B is not duplicated on session 2 — only A (the genuinely
// failed record) is re-sent.
TEST(TPushBasedShuffleWriterTest, SuccessAckOnDyingSessionNotDuplicated)
{
    constexpr int PartitionCount = 1;
    TWriterHarness h;
    auto writer = h.CreateWriter(
        PartitionCount,
        /*memoryBudget*/ 200_KB,
        /*mapperId*/ 17,
        /*buildersBudgetFraction*/ 0.4);

    std::vector<TUnversionedRow> batch0;
    for (int rowIndex = 0; rowIndex < 5000; ++rowIndex) {
        batch0.push_back(h.MakeRow(0, rowIndex));
    }
    WaitFor(writer->Write(TRange(batch0))).ThrowOnError();
    h.DrainInvoker();

    std::vector<TUnversionedRow> batch1;
    for (int rowIndex = 0; rowIndex < 5000; ++rowIndex) {
        batch1.push_back(h.MakeRow(0, 5000 + rowIndex));
    }
    WaitFor(writer->Write(TRange(batch1))).ThrowOnError();
    h.DrainInvoker();

    auto session1 = MakeSessionDescriptor(1, "node-1");
    h.GetProvider()->ProvideSession(0, session1);
    h.DrainInvoker();

    auto writer1 = h.GetChunkWriters().at(session1.SessionId);
    int recordsOnSession1 = std::ssize(writer1->GetRecords());
    ASSERT_GE(recordsOnSession1, 2);

    // First ack: failure. Retires session 1, requests session 2.
    writer1->Ack(TError("simulated"));
    h.DrainInvoker();

    // Second ack: success. Fires while session 2 has not yet resolved. The
    // successful OnWriteResponse removes its cookie from InFlight, so the upcoming
    // sweep at OnSessionResolved will not re-pend it onto session 2.
    writer1->Ack();
    h.DrainInvoker();

    auto session2 = MakeSessionDescriptor(2, "node-2");
    h.GetProvider()->ProvideSession(0, session2);
    h.DrainInvoker();

    auto writer2 = h.GetChunkWriters().at(session2.SessionId);
    // Of the recordsOnSession1 records sent to session 1, one was failed
    // (re-pended to Pending) and one was successfully acked (removed from
    // InFlight). The remaining recordsOnSession1 - 2 are still in InFlight on
    // session 1 when session 2 resolves; the sweep moves them to Pending.
    // Total on session 2: 1 (failed) + (recordsOnSession1 - 2) (swept) =
    // recordsOnSession1 - 1. The successful ack prevented exactly one
    // duplicate.
    EXPECT_EQ(std::ssize(writer2->GetRecords()), recordsOnSession1 - 1)
        << "the successfully-acked record must not be re-sent on session 2";

    // Drain the remaining writer1 promises (stale; OnWriteResponse no-ops for them) so
    // the subscribed callbacks fire and drop their MakeStrong(this) refs.
    // Without this the writer leaks through pending subscriber callbacks.
    while (writer1->GetUnackedCount() > 0) {
        writer1->Ack();
        h.DrainInvoker();
    }

    auto closeFuture = writer->Close();
    h.DrainInvoker();
    while (writer2->GetUnackedCount() > 0) {
        writer2->Ack();
        h.DrainInvoker();
    }
    EXPECT_TRUE(closeFuture.GetOrCrash().IsOK());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NPushBasedShuffleClient
