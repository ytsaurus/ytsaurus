#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/public.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

#include <yt/yt/flow/library/cpp/computation/stores/compact_output_store.h>

#include <yt/yt/flow/library/cpp/tables/compact_output_messages.h>
#include <yt/yt/flow/library/cpp/tables/compact_partition_output_messages.h>
#include <yt/yt/flow/library/cpp/tables/context.h>

#include <yt/yt/flow/library/cpp/common/unittests/mock/time_provider.h>

#include <yt/yt/flow/library/cpp/misc/load_throughput_throttler.h>
#include <yt/yt/flow/library/cpp/misc/retryable_client.h>
#include <yt/yt/flow/library/cpp/misc/retryable_client_spec.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/misc/guid.h>

#include <yt/yt/core/ypath/helpers.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/cache/rpc.h>

namespace NYT::NFlow {
namespace {

using namespace NApi;
using namespace NConcurrency;
using namespace NLogging;
using namespace NObjectClient;
using namespace NYPath;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "Test");

////////////////////////////////////////////////////////////////////////////////

class TTestCompactOutputStore
    : public ::testing::Test
{
protected:
    NApi::IClientPtr Client_;

    void SetUp() override
    {
        // Connect through the RPC proxy using YT_PROXY (exported by the Flow recipe), so the
        // pipeline node type handler that bootstraps a pipeline's child tables runs from the
        // package binaries rather than from the code linked into this test binary.
        Client_ = NClient::NCache::CreateClient();

        WaitFor(Client_->CreateNode(GetPipelinePath(), EObjectType::Pipeline))
            .ThrowOnError();
        WaitUntilTableMounted(YPathJoin(GetPipelinePath(), "compact_partition_output_messages"));
        WaitUntilTableMounted(YPathJoin(GetPipelinePath(), "compact_output_messages"));
    }

    void TearDown() override
    {
        auto tryRemove = [&] {
            return WaitFor(Client_->RemoveNode(GetPipelinePath())).IsOK();
        };
        WaitForPredicate(tryRemove, /*iterationCount*/ 100, /*period*/ TDuration::MilliSeconds(200));
    }

    static TYPath GetPipelinePath()
    {
        return "//pipeline_compact_output_store";
    }

    void WaitUntilTableMounted(const TYPath& tablePath)
    {
        WaitForPredicate(
            [&] {
                auto state = WaitFor(Client_->GetNode(tablePath + "/@tablet_state")).ValueOrThrow();
                return NYTree::ConvertTo<std::string>(state) == "mounted";
            },
            /*iterationCount*/ 100,
            /*period*/ TDuration::MilliSeconds(500));
    }

    // Builds a minimal TComputationStreamSpecStorage for the given stream/schema.
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

    TCompactOutputStoreContextPtr PrepareOutputStoreContext()
    {
        auto partition = New<TPartition>();
        partition->PartitionId = TPartitionId(TGuid::Create());
        partition->ComputationId = TComputationId("TestComputation");
        partition->LowerKey = MakeKey<ui64>(0);
        partition->UpperKey = MakeKey<ui64>(100);

        auto tablesContext = New<NTables::TContext>();
        tablesContext->Client = Client_;
        tablesContext->PipelinePath = GetPipelinePath();
        tablesContext->LoadThroughputThrottler = New<TLoadThroughputThrottler>(
            CreateNamedUnlimitedThroughputThrottler("test", NProfiling::TProfiler()),
            Logger(),
            NProfiling::TProfiler());
        tablesContext->Logger = Logger();
        tablesContext->Profiler = NProfiling::TProfiler();

        auto context = New<TCompactOutputStoreContext>();
        context->Logger = Logger();
        context->Profiler = NProfiling::TProfiler();
        context->Partition = std::move(partition);
        context->OutputStreamIds = {TStreamId("test-output")};
        context->WatermarkPercentileSpec = New<TWatermarkPercentileSpec>();
        context->StreamSpecStorage = MakeStreamSpecStorage(TStreamId("test-output"), Schema_);
        context->CompactPartitionOutputMessagesTable = NTables::CreateCompactPartitionOutputMessages(
            tablesContext,
            New<TDynamicTableRequestSpec>());
        context->CompactOutputMessagesTable = NTables::CreateCompactOutputMessages(
            tablesContext,
            New<TDynamicTableRequestSpec>());
        context->TimeProvider = New<TFakeTimeProvider>();
        return context;
    }

    // Builds a valid TOutputMessageConstPtr using the shared Schema_ pointer.
    TOutputMessageConstPtr MakeMessage(
        const std::string& messageId,
        ui64 systemTimestamp = 100)
    {
        TMessageBuilder builder(TStreamId("test-output"), Schema_);
        builder.SetMessageId(TMessageId(messageId));
        builder.SetSystemTimestamp(TSystemTimestamp(systemTimestamp));
        builder.SetAlignmentTimestamp(TSystemTimestamp(systemTimestamp));
        builder.SetEventTimestamp(TSystemTimestamp(systemTimestamp));
        return New<TOutputMessage>(builder.Finish(), MakeStreamSpecStorage(TStreamId("test-output"), Schema_));
    }

    // Commits a Sync() call inside a dynamic table transaction.
    void SyncStore(const IOutputStorePtr& store)
    {
        auto tx = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet))
            .ValueOrThrow();
        store->Sync(tx);
        WaitFor(tx->Commit()).ThrowOnError();
    }

    // Shared schema pointer — must be the same object used in both
    // TStreamSpecs registration and TMessageBuilder, so that ToProto()
    // can look up the schema pointer in the StreamSpecs map.
    const TTableSchemaPtr Schema_ = New<TTableSchema>();
};

////////////////////////////////////////////////////////////////////////////////

// Verifies the full Register → Sync cycle against a real YT cluster.
// After Register(persist=true) + Sync(), the message is written to the
// compact partition output messages table and Contains() returns true.
TEST_W(TTestCompactOutputStore, RegisterSyncAndInit)
{
    auto context = PrepareOutputStoreContext();
    auto store = CreateCompactOutputStore(context, New<TDynamicOutputStoreSpec>());

    WaitFor(store->Init(/*loadKeyState*/ false)).ThrowOnError();

    auto msg = MakeMessage("msg-1", /*systemTimestamp*/ 100);
    EXPECT_FALSE(store->Contains(*msg));

    store->RegisterBatch(std::array{msg}, /*persist*/ true);
    EXPECT_TRUE(store->Contains(*msg));

    SyncStore(store);

    // Create a new store instance on the same context and Init() it.
    // The previously written message must be loaded and Contains() must return true.
    auto store2 = CreateCompactOutputStore(context, New<TDynamicOutputStoreSpec>());
    auto loaded = WaitFor(store2->Init(/*loadKeyState*/ false)).ValueOrThrow();

    ASSERT_EQ(std::ssize(loaded), 1);
    EXPECT_EQ(loaded[0].first->MessageId, TMessageId("msg-1"));
    EXPECT_FALSE(loaded[0].second.has_value()); // Unkeyed — no key.
    EXPECT_TRUE(store2->Contains(*msg));
}

// Verifies that Register(persist=true) + Sync() + Unregister() + Sync()
// erases the chunk row from the compact partition output messages table.
TEST_W(TTestCompactOutputStore, RegisterUnregisterSync)
{
    auto context = PrepareOutputStoreContext();
    auto store = CreateCompactOutputStore(context, New<TDynamicOutputStoreSpec>());

    WaitFor(store->Init(/*loadKeyState*/ false)).ThrowOnError();

    auto msg1 = MakeMessage("msg-2", /*systemTimestamp*/ 100);
    auto msg2 = MakeMessage("msg-3", /*systemTimestamp*/ 101);
    store->RegisterBatch(std::array{msg1, msg2}, /*persist*/ true);
    SyncStore(store);

    EXPECT_TRUE(store->Contains(*msg1));
    EXPECT_TRUE(store->Contains(*msg2));

    // Unregister first — chunk still present (only msg1 bit flipped in mask).
    store->TryUnregisterBatch(std::array{&msg1->GetMeta()});
    SyncStore(store);
    EXPECT_FALSE(store->Contains(*msg1));
    EXPECT_TRUE(store->Contains(*msg2));

    // A fresh store must see only msg2 (mask filtered msg1 out on Init).
    {
        auto storeCheck = CreateCompactOutputStore(context, New<TDynamicOutputStoreSpec>());
        auto loaded = WaitFor(storeCheck->Init(/*loadKeyState*/ false)).ValueOrThrow();
        ASSERT_EQ(std::ssize(loaded), 1);
        EXPECT_EQ(loaded[0].first->MessageId, TMessageId("msg-3"));
    }

    // Unregister second — full mask ⇒ chunk row erased.
    store->TryUnregisterBatch(std::array{&msg2->GetMeta()});
    SyncStore(store);

    auto store3 = CreateCompactOutputStore(context, New<TDynamicOutputStoreSpec>());
    auto loaded = WaitFor(store3->Init(/*loadKeyState*/ false)).ValueOrThrow();
    EXPECT_TRUE(loaded.empty());
}

// Keyed full cycle for the compact_output_messages table.
TEST_W(TTestCompactOutputStore, RegisterKeyedSyncAndInit)
{
    auto context = PrepareOutputStoreContext();
    auto store = CreateCompactOutputStore(context, New<TDynamicOutputStoreSpec>());

    WaitFor(store->Init(/*loadKeyState*/ true)).ThrowOnError();

    auto msg = MakeMessage("msg-keyed", /*systemTimestamp*/ 100);
    auto key = MakeKey<ui64>(42);
    store->TryRegisterKeyedBatch(std::array{msg}, key, /*persist*/ true);
    SyncStore(store);

    auto store2 = CreateCompactOutputStore(context, New<TDynamicOutputStoreSpec>());
    auto loaded = WaitFor(store2->Init(/*loadKeyState*/ true)).ValueOrThrow();
    ASSERT_EQ(std::ssize(loaded), 1);
    EXPECT_EQ(loaded[0].first->MessageId, TMessageId("msg-keyed"));
    ASSERT_TRUE(loaded[0].second.has_value());
    EXPECT_EQ(*loaded[0].second, key);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
