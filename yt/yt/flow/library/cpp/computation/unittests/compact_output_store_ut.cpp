#include <yt/yt/flow/library/cpp/computation/stores/compact_output_store.h>

#include <yt/yt/flow/library/cpp/common/unittests/mock/time_provider.h>

#include <yt/yt/flow/library/cpp/tables/unittests/mock/compact_output_messages.h>
#include <yt/yt/flow/library/cpp/tables/unittests/mock/compact_partition_output_messages.h>

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/message_batch.h>
#include <yt/yt/flow/library/cpp/common/public.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

#include <yt/yt/flow/library/cpp/misc/retryable_transaction.h>

#include <yt/yt/core/misc/guid.h>
#include <yt/yt/core/test_framework/framework.h>

#include <random>

namespace NYT::NFlow {
namespace {

using namespace NConcurrency;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TCompactOutputStoreTest
    : public ::testing::Test
{
protected:
    const TComputationId ComputationId = TComputationId("test-computation");
    const TPartitionId PartitionId = TPartitionId(TGuid::Create());
    const TStreamId OutputStreamId = TStreamId("test-output");

    // Shared schema pointer — must be the same object used in both
    // TStreamSpecs registration and TMessageBuilder, so that ToProto()
    // can look up the schema pointer in the StreamSpecs map.
    const TTableSchemaPtr Schema_ = New<TTableSchema>();

    // Builds a minimal TComputationStreamSpecStorage for the given stream/schema.
    // EvaluatorCache is passed as nullptr — ComputeKey() is never called in unit tests
    // (Init() only calls it when loading messages, and our mock tables start empty).
    TComputationStreamSpecStoragePtr MakeStreamSpecStorage(
        const TStreamId& streamId,
        const TTableSchemaPtr& schema)
    {
        auto streamSpec = New<TStreamSpec>();
        streamSpec->Schema = schema;

        THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>> specs;
        specs[streamId][TStreamSpecId(1)] = streamSpec;

        auto streamSpecs = New<TStreamSpecs>(specs);
        return New<TComputationStreamSpecStorage>(
            std::move(streamSpecs),
            /*groupBySchema*/ New<TTableSchema>(),
            /*evaluatorCache*/ nullptr);
    }

    TCompactOutputStoreContextPtr MakeContext(
        NTables::TInMemoryCompactPartitionOutputMessagesPtr partitionTable = nullptr,
        NTables::TInMemoryCompactOutputMessagesPtr keyTable = nullptr)
    {
        auto partition = New<TPartition>();
        partition->ComputationId = ComputationId;
        partition->PartitionId = PartitionId;
        partition->LowerKey = MakeKey<ui64>(0);
        partition->UpperKey = MakeKey<ui64>(100);

        auto context = New<TCompactOutputStoreContext>();
        context->Partition = std::move(partition);
        context->OutputStreamIds = {OutputStreamId};
        context->Logger = NLogging::TLogger("Test");
        context->Profiler = NProfiling::TProfiler();
        context->WatermarkPercentileSpec = New<TWatermarkPercentileSpec>();
        context->StreamSpecStorage = MakeStreamSpecStorage(OutputStreamId, Schema_);
        context->CompactPartitionOutputMessagesTable =
            partitionTable ? partitionTable : New<NTables::TInMemoryCompactPartitionOutputMessages>();
        context->CompactOutputMessagesTable =
            keyTable ? keyTable : New<NTables::TInMemoryCompactOutputMessages>();
        context->TimeProvider = New<TFakeTimeProvider>();
        return context;
    }

    // Builds a valid TOutputMessageConstPtr using the shared Schema_ pointer.
    // The schema pointer must match the one registered in TStreamSpecs
    // so that ToProto() can look it up during Sync().
    TOutputMessageConstPtr MakeMessage(
        const std::string& messageId,
        ui64 systemTimestamp = 100)
    {
        TMessageBuilder builder(OutputStreamId, Schema_);
        builder.SetMessageId(TMessageId(messageId));
        builder.SetSystemTimestamp(TSystemTimestamp(systemTimestamp));
        builder.SetAlignmentTimestamp(TSystemTimestamp(systemTimestamp));
        builder.SetEventTimestamp(TSystemTimestamp(systemTimestamp));
        return New<TOutputMessage>(builder.Finish(), MakeStreamSpecStorage(OutputStreamId, Schema_));
    }

    // Parses a chunk's serialized batch back into messages, using a stream specs that
    // matches the one used to serialize it (same stream id / schema pointer).
    std::deque<TMessage> ParseChunk(const TSharedRef& data)
    {
        return ParseMessageBatch(data, MakeStreamSpecStorage(OutputStreamId, Schema_)->GetStreamSpecs());
    }
};

////////////////////////////////////////////////////////////////////////////////

// Init with loadKeyState=false loads only partition messages (unkeyed).
// Both tables are empty, so the result is empty.
TEST_F(TCompactOutputStoreTest, InitReturnsEmpty)
{
    auto context = MakeContext();
    auto store = CreateCompactOutputStore(context, New<TDynamicOutputStoreSpec>());

    auto result = WaitFor(store->Init(/*loadKeyState*/ false));
    EXPECT_TRUE(result.IsOK());
    EXPECT_TRUE(result.Value().empty());
}

// Init with loadKeyState=true loads both partition and keyed messages.
// Both tables are empty, so the result is still empty.
TEST_F(TCompactOutputStoreTest, InitWithKeyStateReturnsEmpty)
{
    auto context = MakeContext();
    auto store = CreateCompactOutputStore(context, New<TDynamicOutputStoreSpec>());

    auto result = WaitFor(store->Init(/*loadKeyState*/ true));
    EXPECT_TRUE(result.IsOK());
    EXPECT_TRUE(result.Value().empty());
}

// Init with loadKeyState=false must NOT load keyed messages from the key table,
// even if the table is pre-filled.
TEST_F(TCompactOutputStoreTest, InitWithLoadKeyStateFalseDoesNotLoadKeyedMessages)
{
    auto keyTable = New<NTables::TInMemoryCompactOutputMessages>();
    auto context = MakeContext(/*partitionTable*/ nullptr, keyTable);

    // Pre-fill the key table with a keyed message before Init().
    {
        auto store = CreateCompactOutputStore(context, New<TDynamicOutputStoreSpec>());
        WaitFor(store->Init(/*loadKeyState*/ true)).ThrowOnError();

        auto msg = MakeMessage("msg-keyed");
        auto key = MakeKey("some-key");
        store->TryRegisterKeyedBatch(std::array{msg}, key, /*persist*/ true);
        EXPECT_TRUE(store->Sync(/*tx*/ nullptr).empty());
    }

    // Verify the key table is non-empty.
    auto loaded = WaitFor(keyTable->LoadAll({.ComputationId = ComputationId})).ValueOrThrow();
    ASSERT_EQ(std::ssize(loaded), 1);

    // Now Init with loadKeyState=false — keyed messages must NOT be returned.
    auto store2 = CreateCompactOutputStore(context, New<TDynamicOutputStoreSpec>());
    auto result = WaitFor(store2->Init(/*loadKeyState*/ false)).ValueOrThrow();
    EXPECT_TRUE(result.empty());

    // Contains() must also return false for the pre-filled keyed message.
    auto msg = MakeMessage("msg-keyed");
    EXPECT_FALSE(store2->Contains(*msg));
}

TEST_F(TCompactOutputStoreTest, ContainsAfterRegister)
{
    auto context = MakeContext();
    auto store = CreateCompactOutputStore(context, New<TDynamicOutputStoreSpec>());
    WaitFor(store->Init(/*loadKeyState*/ true)).ThrowOnError();

    auto msg = MakeMessage("msg-1");
    EXPECT_FALSE(store->Contains(*msg));

    store->RegisterBatch(std::array{msg}, /*persist*/ false);
    EXPECT_TRUE(store->Contains(*msg));
}

// Unkeyed (partition) message: RegisterBatch(persist=true) + Sync() writes a chunk to the partition table.
TEST_F(TCompactOutputStoreTest, RegisterAndSyncWritesToPartitionTable)
{
    auto partitionTable = New<NTables::TInMemoryCompactPartitionOutputMessages>();
    auto context = MakeContext(partitionTable);
    auto store = CreateCompactOutputStore(context, New<TDynamicOutputStoreSpec>());
    WaitFor(store->Init(/*loadKeyState*/ true)).ThrowOnError();

    auto msg = MakeMessage("msg-1");
    store->RegisterBatch(std::array{msg}, /*persist*/ true);

    // Before Sync — nothing in the table yet.
    auto loadBefore = WaitFor(partitionTable->LoadAll({.PartitionId = PartitionId})).ValueOrThrow();
    EXPECT_TRUE(loadBefore.empty());

    // After Sync — one chunk row with the message inside.
    EXPECT_TRUE(store->Sync(/*tx*/ nullptr).empty());

    auto loadAfter = WaitFor(partitionTable->LoadAll({.PartitionId = PartitionId})).ValueOrThrow();
    ASSERT_EQ(std::ssize(loadAfter), 1);
    auto parsed = ParseChunk(loadAfter[0].Data);
    ASSERT_EQ(std::ssize(parsed), 1);
    EXPECT_EQ(parsed[0].MessageId, TMessageId("msg-1"));
}

// Keyed message: RegisterKeyed(persist=true) + Sync() writes a chunk to the key table.
// loadKeyState=true is used because keyed messages are loaded from the key table on Init().
TEST_F(TCompactOutputStoreTest, RegisterKeyedAndSyncWritesToKeyTable)
{
    auto keyTable = New<NTables::TInMemoryCompactOutputMessages>();
    auto context = MakeContext(/*partitionTable*/ nullptr, keyTable);
    auto store = CreateCompactOutputStore(context, New<TDynamicOutputStoreSpec>());
    WaitFor(store->Init(/*loadKeyState*/ true)).ThrowOnError();

    auto msg = MakeMessage("msg-keyed");
    auto key = MakeKey("some-key");
    store->TryRegisterKeyedBatch(std::array{msg}, key, /*persist*/ true);

    // Before Sync — nothing in the key table.
    auto loadBefore = WaitFor(keyTable->LoadAll({.ComputationId = ComputationId})).ValueOrThrow();
    EXPECT_TRUE(loadBefore.empty());

    // After Sync — one chunk row with the message inside.
    EXPECT_TRUE(store->Sync(/*tx*/ nullptr).empty());

    auto loadAfter = WaitFor(keyTable->LoadAll({.ComputationId = ComputationId})).ValueOrThrow();
    ASSERT_EQ(std::ssize(loadAfter), 1);
    EXPECT_EQ(loadAfter[0].Key.Key, key);
    auto parsed = ParseChunk(loadAfter[0].Data);
    ASSERT_EQ(std::ssize(parsed), 1);
    EXPECT_EQ(parsed[0].MessageId, TMessageId("msg-keyed"));
}

// Unkeyed: Unregister() + Sync() erases from partition table.
TEST_F(TCompactOutputStoreTest, UnregisterErasesFromPartitionTable)
{
    auto partitionTable = New<NTables::TInMemoryCompactPartitionOutputMessages>();
    auto context = MakeContext(partitionTable);
    auto store = CreateCompactOutputStore(context, New<TDynamicOutputStoreSpec>());
    WaitFor(store->Init(/*loadKeyState*/ true)).ThrowOnError();

    auto msg = MakeMessage("msg-1");
    store->RegisterBatch(std::array{msg}, /*persist*/ true);
    EXPECT_TRUE(store->Sync(/*tx*/ nullptr).empty());

    // Verify it's in the table.
    auto loadAfterWrite = WaitFor(partitionTable->LoadAll({.PartitionId = PartitionId})).ValueOrThrow();
    ASSERT_EQ(std::ssize(loadAfterWrite), 1);

    // Unregister and sync — should erase from table.
    store->TryUnregisterBatch(std::array{&msg->GetMeta()});
    EXPECT_TRUE(store->Sync(/*tx*/ nullptr).empty());

    auto loadAfterErase = WaitFor(partitionTable->LoadAll({.PartitionId = PartitionId})).ValueOrThrow();
    EXPECT_TRUE(loadAfterErase.empty());
}

// Keyed: Unregister() + Sync() erases from key table.
// loadKeyState=true is used because keyed messages are loaded from the key table on Init().
TEST_F(TCompactOutputStoreTest, UnregisterKeyedErasesFromKeyTable)
{
    auto keyTable = New<NTables::TInMemoryCompactOutputMessages>();
    auto context = MakeContext(/*partitionTable*/ nullptr, keyTable);
    auto store = CreateCompactOutputStore(context, New<TDynamicOutputStoreSpec>());
    WaitFor(store->Init(/*loadKeyState*/ true)).ThrowOnError();

    auto msg = MakeMessage("msg-keyed");
    auto key = MakeKey("some-key");
    store->TryRegisterKeyedBatch(std::array{msg}, key, /*persist*/ true);
    EXPECT_TRUE(store->Sync(/*tx*/ nullptr).empty());

    // Verify it's in the table.
    auto loadAfterWrite = WaitFor(keyTable->LoadAll({.ComputationId = ComputationId})).ValueOrThrow();
    ASSERT_EQ(std::ssize(loadAfterWrite), 1);

    // Unregister and sync — should erase from table.
    store->TryUnregisterBatch(std::array{&msg->GetMeta()});
    EXPECT_TRUE(store->Sync(/*tx*/ nullptr).empty());

    auto loadAfterErase = WaitFor(keyTable->LoadAll({.ComputationId = ComputationId})).ValueOrThrow();
    EXPECT_TRUE(loadAfterErase.empty());
}

// Verifies that Sync() clears the internal buffer: a second Sync() without
// a preceding Register() must not write any additional chunks.
TEST_F(TCompactOutputStoreTest, SyncClearsBuffer)
{
    auto partitionTable = New<NTables::TInMemoryCompactPartitionOutputMessages>();
    auto context = MakeContext(partitionTable);
    auto store = CreateCompactOutputStore(context, New<TDynamicOutputStoreSpec>());
    WaitFor(store->Init(/*loadKeyState*/ true)).ThrowOnError();

    auto msg = MakeMessage("msg-1");
    store->RegisterBatch(std::array{msg}, /*persist*/ true);
    EXPECT_TRUE(store->Sync(/*tx*/ nullptr).empty());

    auto writeCountAfterFirstSync = partitionTable->GetWriteChunkCount();

    // Second Sync without Register — buffer must be empty, nothing written.
    EXPECT_TRUE(store->Sync(/*tx*/ nullptr).empty());

    EXPECT_EQ(partitionTable->GetWriteChunkCount(), writeCountAfterFirstSync);
}

TEST_F(TCompactOutputStoreTest, RegisterUnknownStreamThrows)
{
    auto context = MakeContext();
    auto store = CreateCompactOutputStore(context, New<TDynamicOutputStoreSpec>());
    WaitFor(store->Init(/*loadKeyState*/ true)).ThrowOnError();

    auto schema = New<TTableSchema>();
    TMessageBuilder builder(TStreamId("unknown-stream"), schema);
    builder.SetMessageId(TMessageId("msg-unknown"));
    builder.SetSystemTimestamp(TSystemTimestamp(100));
    builder.SetAlignmentTimestamp(TSystemTimestamp(100));
    builder.SetEventTimestamp(TSystemTimestamp(100));
    TOutputMessageConstPtr msg = New<TOutputMessage>(
        builder.Finish(),
        MakeStreamSpecStorage(TStreamId("unknown-stream"), schema));

    EXPECT_THROW(store->RegisterBatch(std::array{msg}), TErrorException);
}

// persist=true: Contains() returns false after Unregister(), and table is empty after Sync().
TEST_F(TCompactOutputStoreTest, ContainsReturnsFalseAfterUnregisterPersist)
{
    auto partitionTable = New<NTables::TInMemoryCompactPartitionOutputMessages>();
    auto context = MakeContext(partitionTable);
    auto store = CreateCompactOutputStore(context, New<TDynamicOutputStoreSpec>());
    WaitFor(store->Init(/*loadKeyState*/ true)).ThrowOnError();

    auto msg = MakeMessage("msg-1");
    store->RegisterBatch(std::array{msg}, /*persist*/ true);
    EXPECT_TRUE(store->Sync(/*tx*/ nullptr).empty());

    // Verify it's in the table.
    ASSERT_EQ(
        std::ssize(WaitFor(partitionTable->LoadAll({.PartitionId = PartitionId})).ValueOrThrow()),
        1);

    EXPECT_TRUE(store->Contains(*msg));
    store->TryUnregisterBatch(std::array{&msg->GetMeta()});
    EXPECT_FALSE(store->Contains(*msg));

    // After Sync the table must be empty.
    EXPECT_TRUE(store->Sync(/*tx*/ nullptr).empty());
    EXPECT_TRUE(
        WaitFor(partitionTable->LoadAll({.PartitionId = PartitionId})).ValueOrThrow().empty());
}

// persist=false: Register() makes Contains() return true, but Sync() writes nothing to the table.
TEST_F(TCompactOutputStoreTest, RegisterPersistFalseDoesNotWriteToTable)
{
    auto partitionTable = New<NTables::TInMemoryCompactPartitionOutputMessages>();
    auto context = MakeContext(partitionTable);
    auto store = CreateCompactOutputStore(context, New<TDynamicOutputStoreSpec>());
    WaitFor(store->Init(/*loadKeyState*/ true)).ThrowOnError();

    auto msg = MakeMessage("msg-1");
    store->RegisterBatch(std::array{msg}, /*persist*/ false);
    EXPECT_TRUE(store->Contains(*msg));

    EXPECT_TRUE(store->Sync(/*tx*/ nullptr).empty());

    // persist=false — nothing should be written to the table.
    EXPECT_EQ(partitionTable->GetWriteChunkCount(), 0);
    EXPECT_TRUE(
        WaitFor(partitionTable->LoadAll({.PartitionId = PartitionId})).ValueOrThrow().empty());
}

// persist=false: Contains() returns false after Unregister().
TEST_F(TCompactOutputStoreTest, ContainsReturnsFalseAfterUnregisterNoPersist)
{
    auto context = MakeContext();
    auto store = CreateCompactOutputStore(context, New<TDynamicOutputStoreSpec>());
    WaitFor(store->Init(/*loadKeyState*/ true)).ThrowOnError();

    auto msg = MakeMessage("msg-1");
    store->RegisterBatch(std::array{msg}, /*persist*/ false);
    EXPECT_TRUE(store->Contains(*msg));

    store->TryUnregisterBatch(std::array{&msg->GetMeta()});
    EXPECT_FALSE(store->Contains(*msg));
}

// TryRegister with persist=true: duplicate does not throw, table still has exactly one chunk.
TEST_F(TCompactOutputStoreTest, TryRegisterDoesNotThrowOnDuplicatePersist)
{
    auto partitionTable = New<NTables::TInMemoryCompactPartitionOutputMessages>();
    auto context = MakeContext(partitionTable);
    auto store = CreateCompactOutputStore(context, New<TDynamicOutputStoreSpec>());
    WaitFor(store->Init(/*loadKeyState*/ true)).ThrowOnError();

    auto msg = MakeMessage("msg-1");
    store->RegisterBatch(std::array{msg}, /*persist*/ true);

    // TryRegisterBatch on already-registered message must not throw.
    EXPECT_NO_THROW(store->TryRegisterBatch(std::array{msg}, /*persist*/ true));

    EXPECT_TRUE(store->Sync(/*tx*/ nullptr).empty());

    // Exactly one chunk in the table (no duplicates).
    auto loaded = WaitFor(partitionTable->LoadAll({.PartitionId = PartitionId})).ValueOrThrow();
    ASSERT_EQ(std::ssize(loaded), 1);
    auto parsed = ParseChunk(loaded[0].Data);
    ASSERT_EQ(std::ssize(parsed), 1);
    EXPECT_EQ(parsed[0].MessageId, TMessageId("msg-1"));
}

// RegisterKeyed after Init(loadKeyState=false) must throw, because key state
// was not loaded and registering keyed messages would be inconsistent.
TEST_F(TCompactOutputStoreTest, RegisterKeyedAfterInitWithoutKeyStateThrows)
{
    auto context = MakeContext();
    auto store = CreateCompactOutputStore(context, New<TDynamicOutputStoreSpec>());
    WaitFor(store->Init(/*loadKeyState*/ false)).ThrowOnError();

    auto msg = MakeMessage("msg-keyed");
    auto key = MakeKey("some-key");
    EXPECT_THROW(store->TryRegisterKeyedBatch(std::array{msg}, key), TErrorException);
}

////////////////////////////////////////////////////////////////////////////////
// Compact-specific tests below: multi-message chunks, processed_mask bitmask,
// chunk packing limits, idempotency under crash recovery.

// Multiple unkeyed messages in the same stream pack into one chunk.
TEST_F(TCompactOutputStoreTest, MultipleMessagesPackedIntoSingleChunk)
{
    auto partitionTable = New<NTables::TInMemoryCompactPartitionOutputMessages>();
    auto context = MakeContext(partitionTable);
    auto store = CreateCompactOutputStore(context, New<TDynamicOutputStoreSpec>());
    WaitFor(store->Init(/*loadKeyState*/ false)).ThrowOnError();

    store->RegisterBatch(std::array{MakeMessage("msg-1"), MakeMessage("msg-2"), MakeMessage("msg-3")}, /*persist*/ true);
    EXPECT_TRUE(store->Sync(/*tx*/ nullptr).empty());

    auto chunks = WaitFor(partitionTable->LoadAll({.PartitionId = PartitionId})).ValueOrThrow();
    ASSERT_EQ(std::ssize(chunks), 1);
    EXPECT_EQ(std::ssize(ParseChunk(chunks[0].Data)), 3);
}

// A chunk is erased only after ALL its messages are unregistered (the
// processed_mask flips bit-by-bit and the row is erased when fully processed).
TEST_F(TCompactOutputStoreTest, ChunkErasedOnlyWhenAllMessagesUnregistered)
{
    auto partitionTable = New<NTables::TInMemoryCompactPartitionOutputMessages>();
    auto context = MakeContext(partitionTable);
    auto store = CreateCompactOutputStore(context, New<TDynamicOutputStoreSpec>());
    WaitFor(store->Init(/*loadKeyState*/ false)).ThrowOnError();

    auto msg1 = MakeMessage("msg-1");
    auto msg2 = MakeMessage("msg-2");
    store->RegisterBatch(std::array{msg1, msg2}, /*persist*/ true);
    EXPECT_TRUE(store->Sync(/*tx*/ nullptr).empty());

    ASSERT_EQ(std::ssize(WaitFor(partitionTable->LoadAll({.PartitionId = PartitionId})).ValueOrThrow()), 1);

    // Unregister first — chunk still present, processed_mask non-empty.
    store->TryUnregisterBatch(std::array{&msg1->GetMeta()});
    EXPECT_TRUE(store->Sync(/*tx*/ nullptr).empty());
    auto afterFirst = WaitFor(partitionTable->LoadAll({.PartitionId = PartitionId})).ValueOrThrow();
    ASSERT_EQ(std::ssize(afterFirst), 1);
    EXPECT_FALSE(afterFirst[0].ProcessedMask.empty());

    // Unregister second — chunk row gone.
    store->TryUnregisterBatch(std::array{&msg2->GetMeta()});
    EXPECT_TRUE(store->Sync(/*tx*/ nullptr).empty());
    EXPECT_TRUE(WaitFor(partitionTable->LoadAll({.PartitionId = PartitionId})).ValueOrThrow().empty());
}

// Init loads messages from chunks and Contains() reports them as inflight.
TEST_F(TCompactOutputStoreTest, InitLoadsMessagesFromChunks)
{
    auto partitionTable = New<NTables::TInMemoryCompactPartitionOutputMessages>();
    auto context = MakeContext(partitionTable);

    {
        auto store = CreateCompactOutputStore(context, New<TDynamicOutputStoreSpec>());
        WaitFor(store->Init(/*loadKeyState*/ false)).ThrowOnError();
        store->RegisterBatch(std::array{MakeMessage("msg-saved")}, /*persist*/ true);
        EXPECT_TRUE(store->Sync(/*tx*/ nullptr).empty());
    }

    auto store2 = CreateCompactOutputStore(context, New<TDynamicOutputStoreSpec>());
    auto loaded = WaitFor(store2->Init(/*loadKeyState*/ false)).ValueOrThrow();
    ASSERT_EQ(std::ssize(loaded), 1);
    EXPECT_EQ(loaded[0].first->MessageId, TMessageId("msg-saved"));
    EXPECT_TRUE(store2->Contains(*loaded[0].first));
}

// Processed bits persisted across restart — already-delivered messages
// must NOT come back during Init().
TEST_F(TCompactOutputStoreTest, InitSkipsProcessedPositions)
{
    auto partitionTable = New<NTables::TInMemoryCompactPartitionOutputMessages>();
    auto context = MakeContext(partitionTable);

    {
        auto store = CreateCompactOutputStore(context, New<TDynamicOutputStoreSpec>());
        WaitFor(store->Init(/*loadKeyState*/ false)).ThrowOnError();
        auto msg1 = MakeMessage("msg-1");
        auto msg2 = MakeMessage("msg-2");
        store->RegisterBatch(std::array{msg1, msg2}, /*persist*/ true);
        EXPECT_TRUE(store->Sync(/*tx*/ nullptr).empty());
        // Mark msg1 as processed, leave msg2 in flight.
        store->TryUnregisterBatch(std::array{&msg1->GetMeta()});
        EXPECT_TRUE(store->Sync(/*tx*/ nullptr).empty());
    }

    // Reload: only msg2 should re-appear.
    auto store2 = CreateCompactOutputStore(context, New<TDynamicOutputStoreSpec>());
    auto loaded = WaitFor(store2->Init(/*loadKeyState*/ false)).ValueOrThrow();
    ASSERT_EQ(std::ssize(loaded), 1);
    EXPECT_EQ(loaded[0].first->MessageId, TMessageId("msg-2"));
}

// After the chunk is persisted, unregistering a subset of its messages
// must route through UpdateMask (mask-only) and NOT re-issue a full Write.
TEST_F(TCompactOutputStoreTest, MaskOnlyUpdateAvoidsRewritingData)
{
    auto partitionTable = New<NTables::TInMemoryCompactPartitionOutputMessages>();
    auto context = MakeContext(partitionTable);
    auto store = CreateCompactOutputStore(context, New<TDynamicOutputStoreSpec>());
    WaitFor(store->Init(/*loadKeyState*/ false)).ThrowOnError();

    auto msg1 = MakeMessage("msg-1");
    auto msg2 = MakeMessage("msg-2");
    store->RegisterBatch(std::array{msg1, msg2}, /*persist*/ true);
    EXPECT_TRUE(store->Sync(/*tx*/ nullptr).empty());

    // The initial Sync packs and writes the chunk in full.
    const auto writesAfterFirstSync = partitionTable->GetWriteChunkCount();
    EXPECT_EQ(writesAfterFirstSync, 1);
    EXPECT_EQ(partitionTable->GetUpdateMaskCount(), 0);

    // Unregister one of the two messages — the chunk row stays (RemainingCount > 0),
    // but its processed_mask flipped. The next Sync must use UpdateMask, not Write.
    store->TryUnregisterBatch(std::array{&msg1->GetMeta()});
    EXPECT_TRUE(store->Sync(/*tx*/ nullptr).empty());

    EXPECT_EQ(partitionTable->GetWriteChunkCount(), writesAfterFirstSync);
    EXPECT_EQ(partitionTable->GetUpdateMaskCount(), 1);

    // The chunk row is still there with a non-empty mask.
    auto chunks = WaitFor(partitionTable->LoadAll({.PartitionId = PartitionId})).ValueOrThrow();
    ASSERT_EQ(std::ssize(chunks), 1);
    EXPECT_FALSE(chunks[0].ProcessedMask.empty());
}

// When a chunk is unregistered before its first Sync, the upcoming full Write
// already carries the new mask — no separate UpdateMask is issued.
TEST_F(TCompactOutputStoreTest, MaskFlipBeforeFirstSyncFoldsIntoWrite)
{
    auto partitionTable = New<NTables::TInMemoryCompactPartitionOutputMessages>();
    auto context = MakeContext(partitionTable);
    auto store = CreateCompactOutputStore(context, New<TDynamicOutputStoreSpec>());
    WaitFor(store->Init(/*loadKeyState*/ false)).ThrowOnError();

    auto msg1 = MakeMessage("msg-1");
    auto msg2 = MakeMessage("msg-2");
    store->RegisterBatch(std::array{msg1, msg2}, /*persist*/ true);
    // Flip a bit before the chunk is ever flushed.
    store->TryUnregisterBatch(std::array{&msg1->GetMeta()});
    EXPECT_TRUE(store->Sync(/*tx*/ nullptr).empty());

    EXPECT_EQ(partitionTable->GetWriteChunkCount(), 1);
    EXPECT_EQ(partitionTable->GetUpdateMaskCount(), 0);
}

// Messages over the 1024-message chunk limit roll over to a fresh chunk.
TEST_F(TCompactOutputStoreTest, ChunkMessageCountLimit)
{
    auto partitionTable = New<NTables::TInMemoryCompactPartitionOutputMessages>();
    auto context = MakeContext(partitionTable);
    auto store = CreateCompactOutputStore(context, New<TDynamicOutputStoreSpec>());
    WaitFor(store->Init(/*loadKeyState*/ false)).ThrowOnError();

    constexpr int ChunkCap = 1024;
    constexpr int Count = ChunkCap + 1;
    std::vector<TOutputMessageConstPtr> batch;
    batch.reserve(Count);
    for (int i = 0; i < Count; ++i) {
        batch.push_back(MakeMessage(Format("msg-%v", i)));
    }
    store->RegisterBatch(batch, /*persist*/ true);
    EXPECT_TRUE(store->Sync(/*tx*/ nullptr).empty());

    auto chunks = WaitFor(partitionTable->LoadAll({.PartitionId = PartitionId})).ValueOrThrow();
    ASSERT_EQ(std::ssize(chunks), 2);
    int total = 0;
    for (const auto& chunk : chunks) {
        const int messageCount = std::ssize(ParseChunk(chunk.Data));
        EXPECT_LE(messageCount, ChunkCap);
        total += messageCount;
    }
    EXPECT_EQ(total, Count);
}

// Re-registering a message already persisted to a chunk is a no-op
// (must not create a second chunk for the same message).
TEST_F(TCompactOutputStoreTest, ReRegisterAfterPersistIsNoOp)
{
    auto partitionTable = New<NTables::TInMemoryCompactPartitionOutputMessages>();
    auto context = MakeContext(partitionTable);
    auto store = CreateCompactOutputStore(context, New<TDynamicOutputStoreSpec>());
    WaitFor(store->Init(/*loadKeyState*/ false)).ThrowOnError();

    auto msg = MakeMessage("msg-1");
    store->RegisterBatch(std::array{msg}, /*persist*/ true);
    EXPECT_TRUE(store->Sync(/*tx*/ nullptr).empty());
    EXPECT_EQ(partitionTable->GetWriteChunkCount(), 1);

    store->RegisterBatch(std::array{msg}, /*persist*/ true);
    EXPECT_TRUE(store->Sync(/*tx*/ nullptr).empty());
    EXPECT_EQ(partitionTable->GetWriteChunkCount(), 1);
}

// Registering keyed without persist, then with persist must keep the
// key (promote Keys_ -> ToPersist_) and write into the keyed table.
TEST_F(TCompactOutputStoreTest, PromoteKeyedFromKeysToPersist)
{
    auto partitionTable = New<NTables::TInMemoryCompactPartitionOutputMessages>();
    auto keyTable = New<NTables::TInMemoryCompactOutputMessages>();
    auto context = MakeContext(partitionTable, keyTable);
    auto store = CreateCompactOutputStore(context, New<TDynamicOutputStoreSpec>());
    WaitFor(store->Init(/*loadKeyState*/ true)).ThrowOnError();

    auto msg = MakeMessage("msg-keyed");
    auto key = MakeKey("some-key");
    store->TryRegisterKeyedBatch(std::array{msg}, key, /*persist*/ false);
    store->TryRegisterKeyedBatch(std::array{msg}, key, /*persist*/ true);
    EXPECT_TRUE(store->Sync(/*tx*/ nullptr).empty());

    EXPECT_EQ(keyTable->GetWriteChunkCount(), 1);
    EXPECT_EQ(partitionTable->GetWriteChunkCount(), 0);
}

// Same keyed message registered twice with the same key is idempotent.
TEST_F(TCompactOutputStoreTest, IdempotentKeyedReRegister)
{
    auto keyTable = New<NTables::TInMemoryCompactOutputMessages>();
    auto context = MakeContext(/*partitionTable*/ nullptr, keyTable);
    auto store = CreateCompactOutputStore(context, New<TDynamicOutputStoreSpec>());
    WaitFor(store->Init(/*loadKeyState*/ true)).ThrowOnError();

    auto msg = MakeMessage("msg-keyed");
    auto key = MakeKey("some-key");
    store->TryRegisterKeyedBatch(std::array{msg}, key, /*persist*/ true);
    store->TryRegisterKeyedBatch(std::array{msg}, key, /*persist*/ true);
    EXPECT_TRUE(store->Sync(/*tx*/ nullptr).empty());

    EXPECT_EQ(keyTable->GetWriteChunkCount(), 1);
}

// Re-registering the same MessageId with a different key fires YT_VERIFY.
TEST_F(TCompactOutputStoreTest, InconsistentKeyChangeAborts)
{
    auto context = MakeContext();
    auto store = CreateCompactOutputStore(context, New<TDynamicOutputStoreSpec>());
    WaitFor(store->Init(/*loadKeyState*/ true)).ThrowOnError();

    auto msg = MakeMessage("msg-keyed");
    store->TryRegisterKeyedBatch(std::array{msg}, MakeKey("key-A"), /*persist*/ false);
    ASSERT_DEATH(
        store->TryRegisterKeyedBatch(std::array{msg}, MakeKey("key-B"), /*persist*/ false),
        "YT_VERIFY");
}

// Registering a keyed message after it was registered unkeyed (or vice
// versa) fires YT_VERIFY — keyed-ness must stay consistent.
TEST_F(TCompactOutputStoreTest, InconsistentKeyednessAborts)
{
    auto context = MakeContext();
    auto store = CreateCompactOutputStore(context, New<TDynamicOutputStoreSpec>());
    WaitFor(store->Init(/*loadKeyState*/ true)).ThrowOnError();

    auto msg = MakeMessage("msg-x");
    store->RegisterBatch(std::array{msg}, /*persist*/ true);
    ASSERT_DEATH(
        store->TryRegisterKeyedBatch(std::array{msg}, MakeKey("some-key"), /*persist*/ true),
        "YT_VERIFY");
}

// Drains a returned async erase transaction into the mock tables: replays the structured ops the
// store's tables registered via IRetryableTransaction::Apply() (the argument is ignored by those
// callbacks). Not draining a retryable models a failed commit — its ops never reach storage.
void DrainAsyncTx(const IRetryableTransactionPtr& asyncTx)
{
    asyncTx->DoAttempt(/*transaction*/ nullptr);
}

////////////////////////////////////////////////////////////////////////////////

// Every erase beyond the epoch-tx budget is drained within the same epoch via the returned async
// transactions — nothing is carried to a later epoch. Fails on a regression to cross-epoch carry.
TEST_F(TCompactOutputStoreTest, AsyncEraseDrainsOverflowSameEpoch)
{
    constexpr int ChunkCount = 5;

    auto partitionTable = New<NTables::TInMemoryCompactPartitionOutputMessages>();
    auto keyTable = New<NTables::TInMemoryCompactOutputMessages>();
    auto context = MakeContext(partitionTable, keyTable);
    context->MaxEraseRowsPerEpochTransaction = 1;
    context->MaxEraseRowsPerAsyncTransaction = 1;

    auto spec = New<TDynamicOutputStoreSpec>();
    spec->MaxChunkMessageCount = NYTree::TSize(1); // One message per chunk ⇒ one row per message.
    auto store = CreateCompactOutputStore(context, spec);
    WaitFor(store->Init(/*loadKeyState*/ false)).ThrowOnError();

    // Epoch A: persist ChunkCount single-message chunks (writes apply on the spot).
    std::vector<TOutputMessageConstPtr> messages;
    for (int i = 0; i < ChunkCount; ++i) {
        messages.push_back(MakeMessage(Format("msg-%v", i)));
    }
    store->RegisterBatch(messages, /*persist*/ true);
    EXPECT_TRUE(store->Sync(/*tx*/ nullptr).empty()); // Writes alone, no erases yet.
    ASSERT_EQ(std::ssize(WaitFor(partitionTable->LoadAll({.PartitionId = PartitionId})).ValueOrThrow()), ChunkCount);

    // Epoch B: deliver all ⇒ ChunkCount erases; only 1 fits the epoch tx, the rest overflow.
    for (const auto& message : messages) {
        store->TryUnregisterBatch(std::array{&message->GetMeta()});
    }
    auto asyncTxs = store->Sync(/*tx*/ nullptr);

    // Drain-all: one erase rode the epoch tx (applied on the spot), ChunkCount-1 in async txs.
    EXPECT_EQ(std::ssize(asyncTxs), ChunkCount - 1);
    EXPECT_EQ(std::ssize(WaitFor(partitionTable->LoadAll({.PartitionId = PartitionId})).ValueOrThrow()), ChunkCount - 1);

    // Draining all async txs + the clear pass removes the rest in the SAME epoch.
    for (const auto& asyncTx : asyncTxs) {
        DrainAsyncTx(asyncTx);
    }
    EXPECT_TRUE(WaitFor(partitionTable->LoadAll({.PartitionId = PartitionId})).ValueOrThrow().empty());

    // A subsequent Sync is a no-op: the dirty sets are empty after a fully-committed epoch.
    EXPECT_TRUE(store->Sync(/*tx*/ nullptr).empty());
    EXPECT_TRUE(WaitFor(partitionTable->LoadAll({.PartitionId = PartitionId})).ValueOrThrow().empty());
}

// A failed (aborted) async erase commit leaves its rows in storage: the host ThrowOnErrors, the job
// restarts, and a fresh Init reloads exactly those un-erased rows (the store's in-memory state was
// already cleared inline in Sync, so recovery relies on YT, not on retained state).
TEST_F(TCompactOutputStoreTest, AsyncEraseFailureLeavesRowsForReload)
{
    constexpr int ChunkCount = 3;

    auto partitionTable = New<NTables::TInMemoryCompactPartitionOutputMessages>();
    auto keyTable = New<NTables::TInMemoryCompactOutputMessages>();
    auto context = MakeContext(partitionTable, keyTable);
    context->MaxEraseRowsPerEpochTransaction = 1;
    context->MaxEraseRowsPerAsyncTransaction = 1;

    auto spec = New<TDynamicOutputStoreSpec>();
    spec->MaxChunkMessageCount = NYTree::TSize(1);
    auto store = CreateCompactOutputStore(context, spec);
    WaitFor(store->Init(/*loadKeyState*/ false)).ThrowOnError();

    std::vector<TOutputMessageConstPtr> messages;
    for (int i = 0; i < ChunkCount; ++i) {
        messages.push_back(MakeMessage(Format("msg-%v", i)));
    }
    store->RegisterBatch(messages, /*persist*/ true);
    EXPECT_TRUE(store->Sync(/*tx*/ nullptr).empty());

    // Deliver all; drain splits into 1 epoch-tx erase (applied on the spot) + (ChunkCount-1) async.
    for (const auto& message : messages) {
        store->TryUnregisterBatch(std::array{&message->GetMeta()});
    }
    auto asyncTxs = store->Sync(/*tx*/ nullptr);
    ASSERT_EQ(std::ssize(asyncTxs), ChunkCount - 1);

    // All async txs are drained except one that is aborted (commit failure) — its ops never reach
    // storage, so its chunk survives in YT for the fresh Init below.
    for (int i = 0; i + 1 < std::ssize(asyncTxs); ++i) {
        DrainAsyncTx(asyncTxs[i]);
    }

    // Exactly the aborted tx's row survives.
    ASSERT_EQ(std::ssize(WaitFor(partitionTable->LoadAll({.PartitionId = PartitionId})).ValueOrThrow()), 1);

    // Restart: a fresh store reloads exactly the un-erased row and can re-emit its erase.
    auto store2 = CreateCompactOutputStore(context, spec);
    auto loaded = WaitFor(store2->Init(/*loadKeyState*/ false)).ValueOrThrow();
    ASSERT_EQ(std::ssize(loaded), 1);

    store2->TryUnregisterBatch(std::array{&loaded[0].first->GetMeta()});
    auto asyncTxs2 = store2->Sync(/*tx*/ nullptr);
    for (const auto& asyncTx : asyncTxs2) {
        DrainAsyncTx(asyncTx);
    }
    EXPECT_TRUE(WaitFor(partitionTable->LoadAll({.PartitionId = PartitionId})).ValueOrThrow().empty());
}

// Hard write-atomicity invariant: new writes always land in the epoch tx, never in an async tx.
TEST_F(TCompactOutputStoreTest, WritesNeverInAsyncTx)
{
    constexpr int OldCount = 3; // Persisted earlier, delivered now ⇒ erase overflow.
    constexpr int NewCount = 2; // Fresh writes in the same epoch as the erase overflow.

    auto partitionTable = New<NTables::TInMemoryCompactPartitionOutputMessages>();
    auto keyTable = New<NTables::TInMemoryCompactOutputMessages>();
    auto context = MakeContext(partitionTable, keyTable);
    context->MaxEraseRowsPerEpochTransaction = 0; // Force ALL erases into async txs.
    context->MaxEraseRowsPerAsyncTransaction = 1;

    auto spec = New<TDynamicOutputStoreSpec>();
    spec->MaxChunkMessageCount = NYTree::TSize(1);
    auto store = CreateCompactOutputStore(context, spec);
    WaitFor(store->Init(/*loadKeyState*/ false)).ThrowOnError();

    std::vector<TOutputMessageConstPtr> oldMessages;
    for (int i = 0; i < OldCount; ++i) {
        oldMessages.push_back(MakeMessage(Format("old-%v", i)));
    }
    store->RegisterBatch(oldMessages, /*persist*/ true);
    EXPECT_TRUE(store->Sync(/*tx*/ nullptr).empty());
    ASSERT_EQ(partitionTable->GetWriteChunkCount(), OldCount);

    // Same epoch: deliver the old messages (erase overflow) AND register new writes.
    for (const auto& message : oldMessages) {
        store->TryUnregisterBatch(std::array{&message->GetMeta()});
    }
    std::vector<TOutputMessageConstPtr> newMessages;
    for (int i = 0; i < NewCount; ++i) {
        newMessages.push_back(MakeMessage(Format("new-%v", i)));
    }
    store->RegisterBatch(newMessages, /*persist*/ true);

    auto asyncTxs = store->Sync(/*tx*/ nullptr);
    ASSERT_EQ(std::ssize(asyncTxs), OldCount); // All erases async, one row each.

    // Every async tx carries erase-only ops (never a write) — it is non-empty but adds no writes.
    for (const auto& asyncTx : asyncTxs) {
        EXPECT_FALSE(asyncTx->IsEmpty());
    }

    // The epoch tx already applied all NewCount writes on the spot; none of the OldCount erases yet.
    EXPECT_EQ(partitionTable->GetWriteChunkCount(), OldCount + NewCount);
    auto afterEpoch = WaitFor(partitionTable->LoadAll({.PartitionId = PartitionId})).ValueOrThrow();
    EXPECT_EQ(std::ssize(afterEpoch), OldCount + NewCount);

    // Draining the async txs changes only deletions — GetWriteChunkCount stays put.
    for (const auto& asyncTx : asyncTxs) {
        DrainAsyncTx(asyncTx);
    }
    EXPECT_EQ(partitionTable->GetWriteChunkCount(), OldCount + NewCount);
    EXPECT_EQ(std::ssize(WaitFor(partitionTable->LoadAll({.PartitionId = PartitionId})).ValueOrThrow()), NewCount);
}

// Unregister is now immediate: a still-pending message unregistered before Sync is dropped from
// ToPersist_ and never written, even when persisted erases ahead of it overflow the epoch budget.
TEST_F(TCompactOutputStoreTest, PendingMessageDroppedWhenUnregisteredBeforeSync)
{
    constexpr int OldCount = 2;

    auto partitionTable = New<NTables::TInMemoryCompactPartitionOutputMessages>();
    auto keyTable = New<NTables::TInMemoryCompactOutputMessages>();
    auto context = MakeContext(partitionTable, keyTable);
    context->MaxEraseRowsPerEpochTransaction = 1;
    context->MaxEraseRowsPerAsyncTransaction = 1;

    auto spec = New<TDynamicOutputStoreSpec>();
    spec->MaxChunkMessageCount = NYTree::TSize(1);
    auto store = CreateCompactOutputStore(context, spec);
    WaitFor(store->Init(/*loadKeyState*/ false)).ThrowOnError();

    std::vector<TOutputMessageConstPtr> oldMessages;
    for (int i = 0; i < OldCount; ++i) {
        oldMessages.push_back(MakeMessage(Format("old-%v", i)));
    }
    store->RegisterBatch(oldMessages, /*persist*/ true);
    EXPECT_TRUE(store->Sync(/*tx*/ nullptr).empty());
    const auto writesAfterOld = partitionTable->GetWriteChunkCount();
    ASSERT_EQ(writesAfterOld, OldCount);

    // Deliver the old messages (their erases overflow), register a new pending message,
    // then unregister it BEFORE Sync — it must be dropped, never written.
    for (const auto& message : oldMessages) {
        store->TryUnregisterBatch(std::array{&message->GetMeta()});
    }
    auto pending = MakeMessage("pending");
    store->RegisterBatch(std::array{pending}, /*persist*/ true);
    store->TryUnregisterBatch(std::array{&pending->GetMeta()});

    auto asyncTxs = store->Sync(/*tx*/ nullptr);

    // The pending message produced no write (dropped); only the OldCount erases were emitted.
    for (const auto& asyncTx : asyncTxs) {
        DrainAsyncTx(asyncTx);
    }

    EXPECT_EQ(partitionTable->GetWriteChunkCount(), writesAfterOld);
    EXPECT_TRUE(WaitFor(partitionTable->LoadAll({.PartitionId = PartitionId})).ValueOrThrow().empty());
    EXPECT_FALSE(store->Contains(*pending));
}

////////////////////////////////////////////////////////////////////////////////

// Randomized stress: exercise the state machine against a parallel reference model, validating
// Contains() after every action and, on every restart, that the reloaded set equals exactly the
// committed-and-live rows. MaxEraseRowsPerEpochTransaction=0 routes all erases through async txs;
// the restart action occasionally aborts the whole drain (models a post-commit erase failure →
// job restart + Init reload), and the model asserts the un-erased rows come back verbatim.
TEST_F(TCompactOutputStoreTest, RandomizedStress)
{
    constexpr int MessageCount = 30;
    constexpr int Iterations = 2000;
    constexpr int CheckEveryIter = 5;

    std::mt19937 rng(/*seed*/ 42);
    auto rnd = [&] (int lo, int hi) {
        return std::uniform_int_distribution<int>(lo, hi)(rng);
    };

    std::vector<TOutputMessageConstPtr> messages;
    messages.reserve(MessageCount);
    for (int i = 0; i < MessageCount; ++i) {
        messages.push_back(MakeMessage(Format("msg-%03d", i)));
    }
    // Keys must fall inside the partition's [0, 100) reload range so keyed chunks reload on Init.
    const std::vector<TKey> keys = {MakeKey<ui64>(10), MakeKey<ui64>(20), MakeKey<ui64>(30)};

    auto partitionTable = New<NTables::TInMemoryCompactPartitionOutputMessages>();
    auto keyTable = New<NTables::TInMemoryCompactOutputMessages>();
    auto context = MakeContext(partitionTable, keyTable);
    context->MaxEraseRowsPerEpochTransaction = 0; // All erases via async txs (clean abort model).
    context->MaxEraseRowsPerAsyncTransaction = 2;

    auto store = CreateCompactOutputStore(context, New<TDynamicOutputStoreSpec>());
    WaitFor(store->Init(/*loadKeyState*/ true)).ThrowOnError();

    // Reference model.
    std::vector<bool> inflight(MessageCount, false);         // Contains() expectation.
    std::vector<bool> committed(MessageCount, false);        // Has a live (unmasked) committed row.
    std::vector<bool> toPersistPending(MessageCount, false); // Queued persist, write not yet committed.
    std::vector<bool> pendingErase(MessageCount, false);     // Unregistered, erase not yet committed.
    // Once registered keyed, must stay keyed (and same key); switching aborts via YT_VERIFY.
    std::vector<std::optional<bool>> wasKeyed(MessageCount);
    std::vector<std::optional<TKey>> assignedKey(MessageCount);
    // RegisterBatch with ensure=true is only valid as the first Register on a message.
    std::vector<bool> everRegistered(MessageCount, false);

    auto checkContains = [&] (int iter) {
        for (int i = 0; i < MessageCount; ++i) {
            ASSERT_EQ(store->Contains(*messages[i]), inflight[i])
                << "msg-" << i << " Contains() mismatch at iter " << iter;
        }
    };

    for (int iter = 0; iter < Iterations; ++iter) {
        const int action = rnd(0, 99);
        const int idx = rnd(0, MessageCount - 1);

        if (action < 35) {
            // Register. Skip re-registering a persisted-then-unregistered message (messy
            // resurrection path the model deliberately avoids).
            if (!inflight[idx] && committed[idx]) {
                continue;
            }
            const bool useKey = rnd(0, 2) > 0;
            const bool persist = rnd(0, 1);

            if (wasKeyed[idx].has_value() && *wasKeyed[idx] != useKey) {
                continue; // Would abort on keyed/unkeyed flip.
            }
            if (useKey) {
                TKey key = assignedKey[idx].value_or(keys[rnd(0, std::ssize(keys) - 1)]);
                if (assignedKey[idx].has_value() && *assignedKey[idx] != key) {
                    continue; // Would abort on key mismatch.
                }
                store->TryRegisterKeyedBatch(std::array{messages[idx]}, key, persist);
                assignedKey[idx] = key;
                wasKeyed[idx] = true;
            } else if (!everRegistered[idx]) {
                store->RegisterBatch(std::array{messages[idx]}, persist);
                wasKeyed[idx] = false;
            } else {
                store->TryRegisterBatch(std::array{messages[idx]}, persist);
                wasKeyed[idx] = false;
            }
            inflight[idx] = true;
            everRegistered[idx] = true;
            if (persist && !committed[idx]) {
                toPersistPending[idx] = true;
            }
        } else if (action < 65) {
            // TryUnregister (immediate).
            if (inflight[idx]) {
                store->TryUnregisterBatch(std::array{&messages[idx]->GetMeta()});
                inflight[idx] = false;
                if (toPersistPending[idx]) {
                    toPersistPending[idx] = false; // Pending-drop: never written.
                } else if (committed[idx]) {
                    pendingErase[idx] = true;
                }
            }
        } else if (action < 90) {
            // Sync (writes apply on the spot) + drain all async erase txs.
            auto asyncTxs = store->Sync(/*tx*/ nullptr);
            for (const auto& asyncTx : asyncTxs) {
                DrainAsyncTx(asyncTx);
            }
            for (int i = 0; i < MessageCount; ++i) {
                if (toPersistPending[i]) {
                    committed[i] = true;
                    toPersistPending[i] = false;
                }
                if (pendingErase[i]) {
                    committed[i] = false;
                    pendingErase[i] = false;
                }
            }
        } else {
            // Sync (writes apply on the spot) + (sometimes) abort the whole drain, then restart.
            auto asyncTxs = store->Sync(/*tx*/ nullptr);
            const bool abortDrain = !asyncTxs.empty() && rnd(0, 2) == 0;
            if (!abortDrain) {
                for (const auto& asyncTx : asyncTxs) {
                    DrainAsyncTx(asyncTx);
                }
            }
            // Writes always commit (epoch tx). Erases commit only when the drain was not aborted.
            for (int i = 0; i < MessageCount; ++i) {
                if (toPersistPending[i]) {
                    committed[i] = true;
                    toPersistPending[i] = false;
                }
                if (!abortDrain && pendingErase[i]) {
                    committed[i] = false;
                    pendingErase[i] = false;
                }
            }

            store = CreateCompactOutputStore(context, New<TDynamicOutputStoreSpec>());
            auto loaded = WaitFor(store->Init(/*loadKeyState*/ true)).ValueOrThrow();

            std::vector<bool> loadedFlag(MessageCount, false);
            for (const auto& [msg, key] : loaded) {
                int found = -1;
                for (int i = 0; i < MessageCount; ++i) {
                    if (messages[i]->MessageId == msg->MessageId) {
                        found = i;
                        break;
                    }
                }
                ASSERT_GE(found, 0) << "loaded message not in pool";
                ASSERT_EQ(key.has_value(), wasKeyed[found].value_or(false))
                    << "msg-" << found << " keyed-ness mismatch on reload";
                if (key) {
                    ASSERT_EQ(*key, *assignedKey[found]) << "msg-" << found << " key mismatch on reload";
                }
                loadedFlag[found] = true;
            }
            // The reloaded set must equal exactly the committed-and-live rows.
            for (int i = 0; i < MessageCount; ++i) {
                ASSERT_EQ(loadedFlag[i], committed[i])
                    << "msg-" << i << " reload mismatch (loaded=" << loadedFlag[i]
                    << ", committed=" << committed[i] << ") at iter " << iter;
                inflight[i] = loadedFlag[i];
                pendingErase[i] = false;
                toPersistPending[i] = false;
            }
        }

        if (iter % CheckEveryIter == 0) {
            checkContains(iter);
        }
    }
    checkContains(Iterations);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
