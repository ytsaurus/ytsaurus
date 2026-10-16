#include <yt/yt/flow/library/cpp/computation/stores/input_store.h>

#include <yt/yt/flow/library/cpp/tables/unittests/mock/input_messages.h>

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/public.h>

#include <yt/yt/core/misc/guid.h>
#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TInputStoreTest
    : public ::testing::Test
{
protected:
    const TComputationId ComputationId = TComputationId("test-computation");
    const TStreamId StreamId = TStreamId("test-stream");

    TInputStoreContextPtr MakeContext(NTables::TInMemoryInputMessagesPtr table = nullptr)
    {
        auto partition = New<TPartition>();
        partition->ComputationId = ComputationId;
        partition->PartitionId = TPartitionId(TGuid::Create());

        auto context = New<TInputStoreContext>();
        context->Partition = std::move(partition);
        context->InputStreamIds = {StreamId};
        context->Logger = NLogging::TLogger("Test");
        context->InputMessagesTable = table ? table : New<NTables::TInMemoryInputMessages>();
        return context;
    }

    // Creates a valid TInputMessage using TMessageBuilder with an empty schema.
    // TPayloadBuilder::Finish() with zero columns produces a non-null TUnversionedOwningRow,
    // which passes the "Payload is undefined" validation in TInputMessage constructor.
    TInputMessageConstPtr MakeMessage(
        const std::string& messageId,
        ui64 systemTimestamp = 100)
    {
        auto schema = New<NTableClient::TTableSchema>();
        TMessageBuilder builder(StreamId, schema);
        builder.SetMessageId(TMessageId(messageId));
        builder.SetSystemTimestamp(TSystemTimestamp(systemTimestamp));
        builder.SetAlignmentTimestamp(TSystemTimestamp(systemTimestamp));
        builder.SetEventTimestamp(TSystemTimestamp(systemTimestamp));
        auto msg = builder.Finish();

        auto key = MakeKey(ToString(messageId));
        return New<TInputMessage>(std::move(msg), std::move(key));
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TInputStoreTest, InitReturnsOK)
{
    auto context = MakeContext();
    auto store = CreateInputStore(context, New<TDynamicInputStoreSpec>());

    auto result = WaitFor(store->Init());
    EXPECT_TRUE(result.IsOK());
}

TEST_F(TInputStoreTest, AdvanceSystemWatermark)
{
    auto context = MakeContext();
    auto store = CreateInputStore(context, New<TDynamicInputStoreSpec>());

    EXPECT_EQ(store->GetSystemWatermark(), ZeroSystemTimestamp);

    store->AdvanceSystemWatermark(TSystemTimestamp(42));
    EXPECT_EQ(store->GetSystemWatermark(), TSystemTimestamp(42));

    store->AdvanceSystemWatermark(TSystemTimestamp(100));
    EXPECT_EQ(store->GetSystemWatermark(), TSystemTimestamp(100));
}

TEST_F(TInputStoreTest, BasicRegisterAndSync)
{
    auto table = New<NTables::TInMemoryInputMessages>();
    auto context = MakeContext(table);
    auto store = CreateInputStore(context, New<TDynamicInputStoreSpec>());

    WaitFor(store->Init()).ThrowOnError();

    auto msg1 = MakeMessage("msg-1");
    auto msg2 = MakeMessage("msg-2");

    store->Register({msg1, msg2});

    // Before Sync — nothing in the table yet.
    EXPECT_FALSE(table->ContainsDirect(
        ComputationId,
        MakeKey(ToString("msg-1")),
        TMessageId("msg-1")));
    EXPECT_FALSE(table->ContainsDirect(
        ComputationId,
        MakeKey(ToString("msg-2")),
        TMessageId("msg-2")));

    // After Sync — messages are written to the table.
    store->Sync(/*tx*/ nullptr);

    EXPECT_TRUE(table->ContainsDirect(
        ComputationId,
        MakeKey(ToString("msg-1")),
        TMessageId("msg-1")));
    EXPECT_TRUE(table->ContainsDirect(
        ComputationId,
        MakeKey(ToString("msg-2")),
        TMessageId("msg-2")));
}

// Verifies that Sync() clears the internal buffer: a second Sync() without
// a preceding Register() must not re-write any messages (write count stays the same).
TEST_F(TInputStoreTest, SyncClearsBuffer)
{
    auto table = New<NTables::TInMemoryInputMessages>();
    auto context = MakeContext(table);
    auto store = CreateInputStore(context, New<TDynamicInputStoreSpec>());

    WaitFor(store->Init()).ThrowOnError();

    auto msg1 = MakeMessage("msg-1");
    store->Register({msg1});
    store->Sync(/*tx*/ nullptr);

    auto writeCountAfterFirstSync = table->GetWriteCount();

    // Second Sync without Register — buffer must be empty, nothing written.
    store->Sync(/*tx*/ nullptr);

    EXPECT_EQ(table->GetWriteCount(), writeCountAfterFirstSync);
}

TEST_F(TInputStoreTest, FilterBySystemWatermark)
{
    auto context = MakeContext();
    auto store = CreateInputStore(context, New<TDynamicInputStoreSpec>());

    WaitFor(store->Init()).ThrowOnError();

    // Watermark = 50: messages with timestamp < 50 are considered Processed.
    store->AdvanceSystemWatermark(TSystemTimestamp(50));

    auto msgOld = MakeMessage("msg-old", /*systemTimestamp*/ 10);  // < 50 → Processed
    auto msgNew = MakeMessage("msg-new", /*systemTimestamp*/ 100); // >= 50 → Unprocessed

    // checkState=false: filter by watermark only, no table lookup.
    auto result = store->Filter({msgOld, msgNew}, /*checkState*/ false);

    ASSERT_EQ(std::ssize(result.Processed), 1);
    ASSERT_EQ(std::ssize(result.Unprocessed), 1);
    EXPECT_EQ(result.Processed[0]->MessageId, TMessageId("msg-old"));
    EXPECT_EQ(result.Unprocessed[0]->MessageId, TMessageId("msg-new"));
}

TEST_F(TInputStoreTest, FilterByInputMessages)
{
    auto table = New<NTables::TInMemoryInputMessages>();
    auto context = MakeContext(table);
    auto store = CreateInputStore(context, New<TDynamicInputStoreSpec>());

    WaitFor(store->Init()).ThrowOnError();

    auto msg1 = MakeMessage("msg-1", /*systemTimestamp*/ 100);
    auto msg2 = MakeMessage("msg-2", /*systemTimestamp*/ 100);

    // Register and sync msg1 — it is now in the table.
    store->Register({msg1});
    store->Sync(/*tx*/ nullptr);

    // Watermark = 0 (does not affect table-based filtering).
    // checkState=true: filter by InputMessages table contents.
    auto result = store->Filter({msg1, msg2}, /*checkState*/ true);

    ASSERT_EQ(std::ssize(result.Processed), 1);
    ASSERT_EQ(std::ssize(result.Unprocessed), 1);
    EXPECT_EQ(result.Processed[0]->MessageId, TMessageId("msg-1"));
    EXPECT_EQ(result.Unprocessed[0]->MessageId, TMessageId("msg-2"));
}

TEST_F(TInputStoreTest, FilterMixed)
{
    auto table = New<NTables::TInMemoryInputMessages>();
    auto context = MakeContext(table);
    auto store = CreateInputStore(context, New<TDynamicInputStoreSpec>());

    WaitFor(store->Init()).ThrowOnError();

    // msg1: in the table → Processed via table lookup.
    // msg2: timestamp < watermark → Processed via watermark.
    // msg3: not in table, timestamp >= watermark → Unprocessed.
    auto msg1 = MakeMessage("msg-1", /*systemTimestamp*/ 100);
    auto msg2 = MakeMessage("msg-2", /*systemTimestamp*/ 10);
    auto msg3 = MakeMessage("msg-3", /*systemTimestamp*/ 100);

    store->Register({msg1});
    store->Sync(/*tx*/ nullptr);

    store->AdvanceSystemWatermark(TSystemTimestamp(50));

    auto result = store->Filter({msg1, msg2, msg3}, /*checkState*/ true);

    ASSERT_EQ(std::ssize(result.Processed), 2);
    ASSERT_EQ(std::ssize(result.Unprocessed), 1);
    EXPECT_EQ(result.Unprocessed[0]->MessageId, TMessageId("msg-3"));
}

// Verifies that messages with the same MessageId but different Key are stored independently.
TEST_F(TInputStoreTest, SameMessageIdDifferentKeyAreIndependent)
{
    auto table = New<NTables::TInMemoryInputMessages>();
    auto context = MakeContext(table);
    auto store = CreateInputStore(context, New<TDynamicInputStoreSpec>());

    WaitFor(store->Init()).ThrowOnError();

    // Two messages with the same MessageId but different keys.
    auto schema = New<NTableClient::TTableSchema>();
    auto makeMsg = [&] (const std::string& keyStr) {
        TMessageBuilder builder(StreamId, schema);
        builder.SetMessageId(TMessageId("shared-id"));
        builder.SetSystemTimestamp(TSystemTimestamp(100));
        builder.SetAlignmentTimestamp(TSystemTimestamp(100));
        builder.SetEventTimestamp(TSystemTimestamp(100));
        auto msg = builder.Finish();
        auto key = MakeKey(keyStr);
        return New<TInputMessage>(std::move(msg), std::move(key));
    };

    auto msgA = makeMsg("key-a");
    auto msgB = makeMsg("key-b");

    // Register only msgA.
    store->Register({msgA});
    store->Sync(/*tx*/ nullptr);

    // msgA is in the table; msgB (same MessageId, different Key) is not.
    EXPECT_TRUE(table->ContainsDirect(
        ComputationId,
        MakeKey("key-a"),
        TMessageId("shared-id")));
    EXPECT_FALSE(table->ContainsDirect(
        ComputationId,
        MakeKey("key-b"),
        TMessageId("shared-id")));

    // Filter: msgA → Processed, msgB → Unprocessed.
    auto result = store->Filter({msgA, msgB}, /*checkState*/ true);
    ASSERT_EQ(std::ssize(result.Processed), 1);
    ASSERT_EQ(std::ssize(result.Unprocessed), 1);
    EXPECT_EQ(result.Processed[0]->Key, MakeKey("key-a"));
    EXPECT_EQ(result.Unprocessed[0]->Key, MakeKey("key-b"));
}

TEST_F(TInputStoreTest, RegisterUnknownStreamThrows)
{
    auto context = MakeContext();
    auto store = CreateInputStore(context, New<TDynamicInputStoreSpec>());

    WaitFor(store->Init()).ThrowOnError();

    // Build a valid message for an unknown stream (not in InputStreamIds).
    auto schema = New<NTableClient::TTableSchema>();
    TMessageBuilder builder(TStreamId("unknown-stream"), schema);
    builder.SetMessageId(TMessageId("msg-unknown"));
    builder.SetSystemTimestamp(TSystemTimestamp(100));
    builder.SetAlignmentTimestamp(TSystemTimestamp(100));
    builder.SetEventTimestamp(TSystemTimestamp(100));
    auto msg = builder.Finish();

    auto key = MakeKey(ToString("msg-unknown"));
    auto inputMsg = New<TInputMessage>(std::move(msg), std::move(key));

    EXPECT_THROW(store->Register({inputMsg}), TErrorException);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
