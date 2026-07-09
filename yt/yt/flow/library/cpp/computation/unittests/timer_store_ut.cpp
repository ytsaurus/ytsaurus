#include <yt/yt/flow/library/cpp/computation/stores/timer_store.h>

#include <yt/yt/flow/library/cpp/tables/unittests/mock/timers.h>

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/public.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/timer.h>

#include <yt/yt/core/misc/guid.h>
#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow {

using namespace NConcurrency;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TTimerStoreTest
    : public ::testing::Test
{
protected:
    const TComputationId ComputationId = TComputationId("test-computation");
    const TStreamId TimerStreamId = TStreamId("test-timer");
    const TStreamId InputStreamId = TStreamId("test-input");

    TTimerStoreContextPtr MakeContext(
        NTables::TInMemoryTimersPtr table = nullptr,
        ETimeType timeType = ETimeType::SystemTime,
        bool deduplicateEqualTimestamps = false)
    {
        auto partition = New<TPartition>();
        partition->ComputationId = ComputationId;
        partition->PartitionId = TPartitionId(TGuid::Create());
        partition->LowerKey = MakeKey<ui64>(0);
        partition->UpperKey = MakeKey<ui64>(100);

        auto timerSpec = New<TTimerSpec>();
        timerSpec->TimeType = timeType;
        timerSpec->DeduplicateEqualTimestamps = deduplicateEqualTimestamps;

        auto context = New<TTimerStoreContext>();
        context->Partition = std::move(partition);
        context->KeySchema = New<TTableSchema>();
        context->StreamsDependency[TimerStreamId].insert(InputStreamId);
        context->TimerSpecs[TimerStreamId] = std::move(timerSpec);
        context->WatermarkPercentileSpec = New<TWatermarkPercentileSpec>();
        context->Logger = NLogging::TLogger("Test");
        context->Profiler = NProfiling::TProfiler();
        context->TimersTable = table ? table : New<NTables::TInMemoryTimers>();
        return context;
    }

    TDynamicTimerStoreContextPtr MakeDynamicContext(bool draining = false)
    {
        auto dynamicContext = New<TDynamicTimerStoreContext>();
        dynamicContext->DynamicTimerStoreSpec = New<TDynamicTimerStoreSpec>();
        dynamicContext->Draining = draining;
        return dynamicContext;
    }

    // Builds a minimal TTimer with the given messageId and triggerTimestamp.
    // KeySchema is empty (0 columns), so Key must also be empty (0 values).
    // MakeKey() with no args creates a valid empty row (not null).
    TTimer MakeTimer(
        const std::string& messageId,
        ui64 triggerTimestamp = 100,
        ui64 systemTimestamp = 100,
        ui64 eventTimestamp = 100)
    {
        TTimer timer;
        timer.MessageId = TMessageId(messageId);
        timer.StreamId = TimerStreamId;
        timer.SystemTimestamp = TSystemTimestamp(systemTimestamp);
        timer.EventTimestamp = TSystemTimestamp(eventTimestamp);
        timer.TriggerTimestamp = TSystemTimestamp(triggerTimestamp);
        timer.KeySchema = New<TTableSchema>();
        // MakeKey() with no args creates a valid empty row matching the empty KeySchema.
        timer.Key = MakeKey();
        return timer;
    }

    // Advances both system and event watermarks for InputStreamId to the given timestamp.
    void SetWatermark(const ITimerStorePtr& store, ui64 timestamp)
    {
        SetWatermarks(store, /*systemTimestamp*/ timestamp, /*eventTimestamp*/ timestamp);
    }

    // Advances system and event watermarks independently for InputStreamId.
    void SetWatermarks(const ITimerStorePtr& store, ui64 systemTimestamp, ui64 eventTimestamp)
    {
        auto watermarks = New<TWatermarks>();
        watermarks->SystemWatermark = TSystemTimestamp(systemTimestamp);
        watermarks->EventWatermark = TSystemTimestamp(eventTimestamp);
        auto watermarkState = New<TWatermarkState>();
        watermarkState->Streams[InputStreamId] = watermarks;
        store->UpdateWatermarkState(watermarkState);
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TTimerStoreTest, InitReturnsOK)
{
    auto context = MakeContext();
    auto store = CreateTimerStore(context, MakeDynamicContext());

    auto result = WaitFor(store->Init());
    EXPECT_TRUE(result.IsOK());
}

TEST_F(TTimerStoreTest, InitialCountIsZero)
{
    auto context = MakeContext();
    auto store = CreateTimerStore(context, MakeDynamicContext());
    WaitFor(store->Init()).ThrowOnError();

    EXPECT_EQ(store->GetCount(), 0);
}

TEST_F(TTimerStoreTest, RegisterIncreasesCount)
{
    auto context = MakeContext();
    auto store = CreateTimerStore(context, MakeDynamicContext());
    WaitFor(store->Init()).ThrowOnError();

    // Use different trigger timestamps to avoid deduplication by (Key, TriggerTimestamp).
    auto timer1 = MakeTimer("timer-1", /*triggerTimestamp*/ 100);
    auto timer2 = MakeTimer("timer-2", /*triggerTimestamp*/ 200);

    store->Register({timer1});
    EXPECT_EQ(store->GetCount(), 1);

    store->Register({timer2});
    EXPECT_EQ(store->GetCount(), 2);
}

TEST_F(TTimerStoreTest, UnregisterDecreasesCount)
{
    auto context = MakeContext();
    auto store = CreateTimerStore(context, MakeDynamicContext());
    WaitFor(store->Init()).ThrowOnError();

    // Use different trigger timestamps to avoid deduplication by (Key, TriggerTimestamp).
    auto timer1 = MakeTimer("timer-1", /*triggerTimestamp*/ 100);
    auto timer2 = MakeTimer("timer-2", /*triggerTimestamp*/ 200);

    store->Register({timer1, timer2});
    EXPECT_EQ(store->GetCount(), 2);

    store->Unregister({New<TInputTimer>(TTimer(timer1))});
    EXPECT_EQ(store->GetCount(), 1);

    store->Unregister({New<TInputTimer>(TTimer(timer2))});
    EXPECT_EQ(store->GetCount(), 0);
}

TEST_F(TTimerStoreTest, RegisterAndSyncWritesToTable)
{
    auto table = New<NTables::TInMemoryTimers>();
    auto context = MakeContext(table);
    auto store = CreateTimerStore(context, MakeDynamicContext());
    WaitFor(store->Init()).ThrowOnError();

    auto timer = MakeTimer("timer-1", /*triggerTimestamp*/ 200);
    store->Register({timer});

    // Before Sync — nothing in the table yet.
    auto loadBefore = WaitFor(table->LoadAll({.ComputationId = ComputationId})).ValueOrThrow();
    EXPECT_TRUE(loadBefore.empty());

    // After Sync — timer is written to the table.
    store->Sync(/*tx*/ nullptr);

    auto loadAfter = WaitFor(table->LoadAll({.ComputationId = ComputationId})).ValueOrThrow();
    ASSERT_EQ(std::ssize(loadAfter), 1);
    EXPECT_EQ(loadAfter[0].second.MessageId, TMessageId("timer-1"));
    EXPECT_EQ(loadAfter[0].second.TriggerTimestamp, TSystemTimestamp(200));
}

TEST_F(TTimerStoreTest, UnregisterAndSyncErasesFromTable)
{
    auto table = New<NTables::TInMemoryTimers>();
    auto context = MakeContext(table);
    auto store = CreateTimerStore(context, MakeDynamicContext());
    WaitFor(store->Init()).ThrowOnError();

    auto timer = MakeTimer("timer-1");
    store->Register({timer});
    store->Sync(/*tx*/ nullptr);

    // Verify it's in the table.
    auto loadAfterWrite = WaitFor(table->LoadAll({.ComputationId = ComputationId})).ValueOrThrow();
    ASSERT_EQ(std::ssize(loadAfterWrite), 1);

    // Unregister and sync — should erase from table.
    store->Unregister({New<TInputTimer>(TTimer(timer))});
    store->Sync(/*tx*/ nullptr);

    auto loadAfterErase = WaitFor(table->LoadAll({.ComputationId = ComputationId})).ValueOrThrow();
    EXPECT_TRUE(loadAfterErase.empty());
}

// Verifies that Sync() clears the internal buffer: a second Sync() without
// a preceding Register() must not write any additional timers.
TEST_F(TTimerStoreTest, SyncClearsBuffer)
{
    auto table = New<NTables::TInMemoryTimers>();
    auto context = MakeContext(table);
    auto store = CreateTimerStore(context, MakeDynamicContext());
    WaitFor(store->Init()).ThrowOnError();

    auto timer = MakeTimer("timer-1");
    store->Register({timer});
    store->Sync(/*tx*/ nullptr);

    auto writeCountAfterFirstSync = table->GetWriteCount();

    // Second Sync without Register — buffer must be empty, nothing written.
    store->Sync(/*tx*/ nullptr);

    EXPECT_EQ(table->GetWriteCount(), writeCountAfterFirstSync);
}

TEST_F(TTimerStoreTest, RegisterUnknownStreamThrows)
{
    auto context = MakeContext();
    auto store = CreateTimerStore(context, MakeDynamicContext());
    WaitFor(store->Init()).ThrowOnError();

    TTimer timer;
    timer.MessageId = TMessageId("timer-unknown");
    timer.StreamId = TStreamId("unknown-stream");
    timer.SystemTimestamp = TSystemTimestamp(100);
    timer.EventTimestamp = TSystemTimestamp(100);
    timer.TriggerTimestamp = TSystemTimestamp(100);
    timer.KeySchema = New<TTableSchema>();
    timer.Key = MakeKey();

    EXPECT_THROW(store->Register({timer}), TErrorException);
}

TEST_F(TTimerStoreTest, GetNextBatchReturnsTriggeredTimers)
{
    auto context = MakeContext();
    auto store = CreateTimerStore(context, MakeDynamicContext());
    WaitFor(store->Init()).ThrowOnError();

    // Timer with triggerTimestamp=50 should be triggered when watermark >= 50.
    auto timer = MakeTimer("timer-1", /*triggerTimestamp*/ 50);
    store->Register({timer});

    // No watermark yet — no triggered timers.
    auto batchBefore = store->GetNextBatch({TimerStreamId}, /*maxRows*/ 100, /*maxByteSize*/ 1 << 20);
    EXPECT_TRUE(batchBefore.empty());

    // Advance watermark past trigger timestamp.
    SetWatermark(store, /*timestamp*/ 100);

    auto batchAfter = store->GetNextBatch({TimerStreamId}, /*maxRows*/ 100, /*maxByteSize*/ 1 << 20);
    ASSERT_EQ(std::ssize(batchAfter), 1);
    EXPECT_EQ(batchAfter[0]->MessageId, TMessageId("timer-1"));
}

TEST_F(TTimerStoreTest, DrainingBlocksGetNextBatch)
{
    auto context = MakeContext();
    auto store = CreateTimerStore(context, MakeDynamicContext());
    WaitFor(store->Init()).ThrowOnError();

    auto timer = MakeTimer("timer-1", /*triggerTimestamp*/ 50);
    store->Register({timer});
    SetWatermark(store, /*timestamp*/ 100);

    // With draining=true, GetNextBatch returns empty.
    store->Reconfigure(MakeDynamicContext(/*draining*/ true));
    auto batch = store->GetNextBatch({TimerStreamId}, /*maxRows*/ 100, /*maxByteSize*/ 1 << 20);
    EXPECT_TRUE(batch.empty());

    // With draining=false, GetNextBatch returns the triggered timer.
    store->Reconfigure(MakeDynamicContext(/*draining*/ false));
    auto batchAfter = store->GetNextBatch({TimerStreamId}, /*maxRows*/ 100, /*maxByteSize*/ 1 << 20);
    ASSERT_EQ(std::ssize(batchAfter), 1);
}

// Timer with TimeType=SystemTime triggers when SystemWatermark >= TriggerTimestamp,
// regardless of EventWatermark.
TEST_F(TTimerStoreTest, SystemTimeTypeTriggersOnSystemWatermark)
{
    auto context = MakeContext(/*table*/ nullptr, ETimeType::SystemTime);
    auto store = CreateTimerStore(context, MakeDynamicContext());
    WaitFor(store->Init()).ThrowOnError();

    auto timer = MakeTimer("timer-1", /*triggerTimestamp*/ 50);
    store->Register({timer});

    // Advance only EventWatermark past trigger — must NOT trigger (TimeType=SystemTime).
    SetWatermarks(store, /*systemTimestamp*/ 10, /*eventTimestamp*/ 100);
    auto batchNoTrigger = store->GetNextBatch({TimerStreamId}, /*maxRows*/ 100, /*maxByteSize*/ 1 << 20);
    EXPECT_TRUE(batchNoTrigger.empty());

    // Advance SystemWatermark past trigger — must trigger.
    SetWatermarks(store, /*systemTimestamp*/ 100, /*eventTimestamp*/ 10);
    auto batchTriggered = store->GetNextBatch({TimerStreamId}, /*maxRows*/ 100, /*maxByteSize*/ 1 << 20);
    ASSERT_EQ(std::ssize(batchTriggered), 1);
    EXPECT_EQ(batchTriggered[0]->MessageId, TMessageId("timer-1"));
}

// Timer with TimeType=EventTime triggers when EventWatermark >= TriggerTimestamp,
// regardless of SystemWatermark.
TEST_F(TTimerStoreTest, EventTimeTypeTriggersOnEventWatermark)
{
    auto context = MakeContext(/*table*/ nullptr, ETimeType::EventTime);
    auto store = CreateTimerStore(context, MakeDynamicContext());
    WaitFor(store->Init()).ThrowOnError();

    auto timer = MakeTimer("timer-1", /*triggerTimestamp*/ 50);
    store->Register({timer});

    // Advance only SystemWatermark past trigger — must NOT trigger (TimeType=EventTime).
    SetWatermarks(store, /*systemTimestamp*/ 100, /*eventTimestamp*/ 10);
    auto batchNoTrigger = store->GetNextBatch({TimerStreamId}, /*maxRows*/ 100, /*maxByteSize*/ 1 << 20);
    EXPECT_TRUE(batchNoTrigger.empty());

    // Advance EventWatermark past trigger — must trigger.
    SetWatermarks(store, /*systemTimestamp*/ 10, /*eventTimestamp*/ 100);
    auto batchTriggered = store->GetNextBatch({TimerStreamId}, /*maxRows*/ 100, /*maxByteSize*/ 1 << 20);
    ASSERT_EQ(std::ssize(batchTriggered), 1);
    EXPECT_EQ(batchTriggered[0]->MessageId, TMessageId("timer-1"));
}

// When multiple timers have the same TriggerTimestamp, GetNextBatch returns them
// in ascending MessageId order (lexicographic).
// DeduplicateEqualTimestamps=false so all three timers with the same (Key, TriggerTimestamp)
// are kept and not collapsed to one.
TEST_F(TTimerStoreTest, SameTriggerTimestampOrderedByMessageId)
{
    auto context = MakeContext(
        /*table*/ nullptr,
        ETimeType::SystemTime,
        /*deduplicateEqualTimestamps*/ false);
    auto store = CreateTimerStore(context, MakeDynamicContext());
    WaitFor(store->Init()).ThrowOnError();

    // Register three timers with the same TriggerTimestamp but different MessageIds.
    // MessageIds are chosen so that lexicographic order differs from registration order.
    auto timerC = MakeTimer("timer-c", /*triggerTimestamp*/ 50);
    auto timerA = MakeTimer("timer-a", /*triggerTimestamp*/ 50);
    auto timerB = MakeTimer("timer-b", /*triggerTimestamp*/ 50);
    store->Register({timerC, timerA, timerB});

    // Advance watermark past trigger timestamp.
    SetWatermark(store, /*timestamp*/ 100);

    auto batch = store->GetNextBatch({TimerStreamId}, /*maxRows*/ 100, /*maxByteSize*/ 1 << 20);
    ASSERT_EQ(std::ssize(batch), 3);

    // Must be in ascending MessageId order regardless of registration order.
    EXPECT_EQ(batch[0]->MessageId, TMessageId("timer-a"));
    EXPECT_EQ(batch[1]->MessageId, TMessageId("timer-b"));
    EXPECT_EQ(batch[2]->MessageId, TMessageId("timer-c"));
}

// When DeduplicateEqualTimestamps=true and two timers share the same (Key, TriggerTimestamp),
// only the one with the lowest EventTimestamp is kept; the other is silently unregistered.
TEST_F(TTimerStoreTest, DeduplicateEqualTimestampsKeepsLowestEventTimestamp)
{
    auto context = MakeContext(
        /*table*/ nullptr,
        ETimeType::SystemTime,
        /*deduplicateEqualTimestamps*/ true);
    auto store = CreateTimerStore(context, MakeDynamicContext());
    WaitFor(store->Init()).ThrowOnError();

    // Two timers with the same (Key, TriggerTimestamp) but different EventTimestamps.
    // timer-early has EventTimestamp=10 (lower), timer-late has EventTimestamp=20 (higher).
    // After deduplication, only timer-early must survive.
    auto timerLate = MakeTimer(
        "timer-late",
        /*triggerTimestamp*/ 50,
        /*systemTimestamp*/ 100,
        /*eventTimestamp*/ 20);
    auto timerEarly = MakeTimer(
        "timer-early",
        /*triggerTimestamp*/ 50,
        /*systemTimestamp*/ 100,
        /*eventTimestamp*/ 10);

    // Register late first, then early — deduplication must still keep early.
    store->Register({timerLate, timerEarly});

    // Only one timer must remain after deduplication.
    EXPECT_EQ(store->GetCount(), 1);

    // Advance watermark to trigger.
    SetWatermark(store, /*timestamp*/ 100);

    auto batch = store->GetNextBatch({TimerStreamId}, /*maxRows*/ 100, /*maxByteSize*/ 1 << 20);
    ASSERT_EQ(std::ssize(batch), 1);
    EXPECT_EQ(batch[0]->MessageId, TMessageId("timer-early"));
}

// Regression: Unregister a triggered timer, then call GetNextBatch.
// The timer is moved from SortedTimers_ to TriggeredTimers_ when the watermark advances.
// Unregister must also remove it from TriggeredTimers_ to avoid a stale entry.
TEST_F(TTimerStoreTest, UnregisterTriggeredTimerThenGetNextBatch)
{
    auto context = MakeContext();
    auto store = CreateTimerStore(context, MakeDynamicContext());
    WaitFor(store->Init()).ThrowOnError();

    auto timer = MakeTimer("timer-1", /*triggerTimestamp*/ 50);
    store->Register({timer});

    // Advance watermark past trigger — timer moves to TriggeredTimers_.
    SetWatermark(store, /*timestamp*/ 100);

    // Unregister the already-triggered timer.
    store->Unregister({New<TInputTimer>(TTimer(timer))});
    EXPECT_EQ(store->GetCount(), 0);

    // GetNextBatch must not crash and must return empty.
    auto batch = store->GetNextBatch({TimerStreamId}, /*maxRows*/ 100, /*maxByteSize*/ 1 << 20);
    EXPECT_TRUE(batch.empty());
}

// Deduplication of a triggered timer must also clean TriggeredTimers_.
// Scenario:
//   1. Register timer T1 with (Key=K, TriggerTimestamp=50).
//   2. Advance watermark — T1 moves to TriggeredTimers_.
//   3. Register timer T2 with the same (Key=K, TriggerTimestamp=50) but lower EventTimestamp.
//      Deduplication calls Unregister(T1), which must remove T1 from TriggeredTimers_ too.
//   4. GetNextBatch must return only T2 without crashing.
TEST_F(TTimerStoreTest, DeduplicateTriggeredTimerThenGetNextBatch)
{
    auto context = MakeContext(
        /*table*/ nullptr,
        ETimeType::SystemTime,
        /*deduplicateEqualTimestamps*/ true);
    auto store = CreateTimerStore(context, MakeDynamicContext());
    WaitFor(store->Init()).ThrowOnError();

    // Step 1: Register T1 with TriggerTimestamp=50.
    auto timerOld = MakeTimer(
        "timer-old",
        /*triggerTimestamp*/ 50,
        /*systemTimestamp*/ 100,
        /*eventTimestamp*/ 20);
    store->Register({timerOld});
    EXPECT_EQ(store->GetCount(), 1);

    // Step 2: Advance watermark — T1 moves to TriggeredTimers_.
    SetWatermark(store, /*timestamp*/ 100);

    // Step 3: Register T2 with the same (Key, TriggerTimestamp) but lower EventTimestamp.
    // Deduplication should keep T2 (lower EventTimestamp) and unregister T1.
    auto timerNew = MakeTimer(
        "timer-new",
        /*triggerTimestamp*/ 50,
        /*systemTimestamp*/ 100,
        /*eventTimestamp*/ 10);
    store->Register({timerNew});

    // After deduplication, only one timer must remain.
    EXPECT_EQ(store->GetCount(), 1);

    // Step 4: Re-trigger watermark so timerNew moves from SortedTimers_ to TriggeredTimers_.
    // (TriggerTimers() is only called inside UpdateWatermarkState.)
    SetWatermark(store, /*timestamp*/ 100);

    // GetNextBatch must not crash and must return only the surviving timer.
    auto batch = store->GetNextBatch({TimerStreamId}, /*maxRows*/ 100, /*maxByteSize*/ 1 << 20);
    ASSERT_EQ(std::ssize(batch), 1);
    EXPECT_EQ(batch[0]->MessageId, TMessageId("timer-new"));
}

// Scenario: GetNextBatch returns timer1, then Register timer2 (duplicate key/timestamp)
// and Unregister timer1. Timer2 must survive and be returned by the next GetNextBatch.
TEST_F(TTimerStoreTest, GetNextBatchThenRegisterDuplicateAndUnregisterOriginal)
{
    auto context = MakeContext();
    auto store = CreateTimerStore(context, MakeDynamicContext());
    WaitFor(store->Init()).ThrowOnError();

    // Step 1: Register timer1 and trigger it.
    auto timer1 = MakeTimer(
        "timer-1",
        /*triggerTimestamp*/ 50,
        /*systemTimestamp*/ 100,
        /*eventTimestamp*/ 100);
    store->Register({timer1});
    SetWatermark(store, /*timestamp*/ 100);

    // Step 2: GetNextBatch returns timer1.
    auto batch1 = store->GetNextBatch({TimerStreamId}, /*maxRows*/ 100, /*maxByteSize*/ 1 << 20);
    ASSERT_EQ(std::ssize(batch1), 1);
    EXPECT_EQ(batch1[0]->MessageId, TMessageId("timer-1"));

    // Step 3: Register timer2 with the same (Key, TriggerTimestamp) as timer1.
    auto timer2 = MakeTimer(
        "timer-2",
        /*triggerTimestamp*/ 50,
        /*systemTimestamp*/ 200,
        /*eventTimestamp*/ 200);
    store->Register({timer2});

    // Step 4: Unregister timer1.
    store->Unregister({New<TInputTimer>(TTimer(timer1))});

    // Timer2 must survive.
    EXPECT_EQ(store->GetCount(), 1);

    // Step 5: Timer2 is already triggered (watermark=100 >= triggerTimestamp=50),
    // so it should appear in the next GetNextBatch.
    // Note: TriggerTimers() is called inside UpdateWatermarkState, so we need to
    // re-trigger to move timer2 from SortedTimers_ to TriggeredTimers_.
    SetWatermark(store, /*timestamp*/ 100);

    auto batch2 = store->GetNextBatch({TimerStreamId}, /*maxRows*/ 100, /*maxByteSize*/ 1 << 20);
    ASSERT_EQ(std::ssize(batch2), 1);
    EXPECT_EQ(batch2[0]->MessageId, TMessageId("timer-2"));
}

////////////////////////////////////////////////////////////////////////////////

// Extracted timers (returned by GetNextBatch) must not participate in deduplication.
// Scenario:
//   1. Register timer1 with (Key=K, TriggerTimestamp=50, EventTimestamp=20), deduplication=true.
//   2. Trigger + GetNextBatch → timer1 is extracted (removed from TimerKeyIndex_).
//   3. Register timer2 with the same (Key=K, TriggerTimestamp=50, EventTimestamp=10).
//      Because timer1 is no longer in the deduplication index, timer2 must NOT evict timer1.
//   4. Both timers must survive (Count == 2).
TEST_F(TTimerStoreTest, ExtractedTimerIsNotDeduplicated)
{
    auto context = MakeContext(
        /*table*/ nullptr,
        ETimeType::SystemTime,
        /*deduplicateEqualTimestamps*/ true);
    auto store = CreateTimerStore(context, MakeDynamicContext());
    WaitFor(store->Init()).ThrowOnError();

    // Step 1: Register timer1.
    auto timer1 = MakeTimer(
        "timer-1",
        /*triggerTimestamp*/ 50,
        /*systemTimestamp*/ 100,
        /*eventTimestamp*/ 20);
    store->Register({timer1});
    EXPECT_EQ(store->GetCount(), 1);

    // Step 2: Trigger and extract timer1 via GetNextBatch.
    SetWatermark(store, /*timestamp*/ 100);
    auto batch1 = store->GetNextBatch({TimerStreamId}, /*maxRows*/ 100, /*maxByteSize*/ 1 << 20);
    ASSERT_EQ(std::ssize(batch1), 1);
    EXPECT_EQ(batch1[0]->MessageId, TMessageId("timer-1"));

    // Step 3: Register timer2 with the same (Key, TriggerTimestamp) but lower EventTimestamp.
    // Without the fix, deduplication would evict timer1 (already extracted!).
    // With the fix, timer1 is no longer in TimerKeyIndex_, so no deduplication occurs.
    auto timer2 = MakeTimer(
        "timer-2",
        /*triggerTimestamp*/ 50,
        /*systemTimestamp*/ 200,
        /*eventTimestamp*/ 10);
    store->Register({timer2});

    // Step 4: Both timers must survive — timer1 was already extracted, timer2 is new.
    EXPECT_EQ(store->GetCount(), 2);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
