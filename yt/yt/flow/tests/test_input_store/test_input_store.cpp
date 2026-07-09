#include <yt/yt/tests/cpp/test_base/api_test_base.h>

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/public.h>

#include <yt/yt/flow/library/cpp/computation/stores/input_store.h>

#include <yt/yt/flow/library/cpp/tables/context.h>
#include <yt/yt/flow/library/cpp/tables/input_messages.h>
#include <yt/yt/flow/library/cpp/tables/transaction_manager.h>

#include <yt/yt/flow/library/cpp/misc/load_throughput_throttler.h>
#include <yt/yt/flow/library/cpp/misc/retryable_client.h>
#include <yt/yt/flow/library/cpp/misc/retryable_client_spec.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/misc/guid.h>

#include <yt/yt/core/ypath/helpers.h>

#include <yt/yt/client/api/transaction.h>

namespace NYT::NFlow {
namespace {

using namespace NApi;
using namespace NConcurrency;
using namespace NCppTests;
using namespace NLogging;
using namespace NObjectClient;
using namespace NYPath;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "Test");

////////////////////////////////////////////////////////////////////////////////

class TTestInputStore
    : public TDynamicTablesTestBase
{
public:
    TTestInputStore() = default;

protected:
    void SetUp() override
    {
        LeaseTransaction_ = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Master))
            .ValueOrThrow();
        ActionQueue_ = New<TActionQueue>();

        WaitFor(Client_->CreateNode(GetPipelinePath(), EObjectType::Pipeline))
            .ThrowOnError();
        WaitUntilEqual(YPathJoin(GetPipelinePath(), "input_messages") + "/@tablet_state", "mounted");
    }

    void TearDown() override
    {
        auto tryRemove = [&] {
            return WaitFor(Client_->RemoveNode(GetPipelinePath())).IsOK();
        };
        WaitForPredicate(tryRemove, /*iterationCount*/ 100, /*period*/ TDuration::MilliSeconds(200));
        if (LeaseTransaction_) {
            WaitFor(LeaseTransaction_->Abort()).ThrowOnError();
            LeaseTransaction_.Reset();
        }
        if (ActionQueue_) {
            ActionQueue_->Shutdown();
        }
    }

    static TYPath GetPipelinePath()
    {
        return "//pipeline_input_store";
    }

    TInputStoreContextPtr PrepareInputStoreContext()
    {
        auto partition = New<TPartition>();
        partition->PartitionId = TPartitionId(TGuid::Create());
        partition->ComputationId = TComputationId("TestComputation");

        auto tablesContext = New<NTables::TContext>();
        tablesContext->Client = Client_;
        tablesContext->PipelinePath = GetPipelinePath();
        tablesContext->LoadThroughputThrottler = New<TLoadThroughputThrottler>(
            CreateNamedUnlimitedThroughputThrottler("test", NProfiling::TProfiler()),
            Logger(),
            NProfiling::TProfiler());
        tablesContext->Logger = Logger();
        tablesContext->Profiler = NProfiling::TProfiler();

        auto context = New<TInputStoreContext>();
        context->Logger = Logger();
        context->Profiler = NProfiling::TProfiler();
        context->Partition = std::move(partition);
        context->InputStreamIds = {TStreamId("test-stream")};
        context->InputMessagesTable = New<NTables::TInputMessages>(tablesContext);
        return context;
    }

    // Creates a valid TInputMessage using TMessageBuilder with an empty schema.
    TInputMessageConstPtr MakeMessage(
        const std::string& messageId,
        ui64 systemTimestamp = 100)
    {
        auto schema = New<NTableClient::TTableSchema>();
        TMessageBuilder builder(TStreamId("test-stream"), schema);
        builder.SetMessageId(TMessageId(messageId));
        builder.SetSystemTimestamp(TSystemTimestamp(systemTimestamp));
        builder.SetAlignmentTimestamp(TSystemTimestamp(systemTimestamp));
        builder.SetEventTimestamp(TSystemTimestamp(systemTimestamp));
        auto msg = builder.Finish();

        auto key = MakeKey(ToString(messageId));
        return New<TInputMessage>(std::move(msg), std::move(key));
    }

    // Commits a Sync() call inside a dynamic table transaction.
    void SyncStore(const IInputStorePtr& store)
    {
        auto tx = WaitFor(Client_->StartTransaction(NTransactionClient::ETransactionType::Tablet))
            .ValueOrThrow();
        store->Sync(tx);
        WaitFor(tx->Commit()).ThrowOnError();
    }

    ITransactionPtr LeaseTransaction_;
    TActionQueuePtr ActionQueue_;
};

////////////////////////////////////////////////////////////////////////////////

// Verifies the full Register → Sync → Filter cycle against a real YT cluster.
TEST_W(TTestInputStore, RegisterSyncAndFilter)
{
    auto context = PrepareInputStoreContext();
    auto store = CreateInputStore(context, New<TDynamicInputStoreSpec>());

    WaitFor(store->Init()).ThrowOnError();

    auto msg1 = MakeMessage("msg-1", /*systemTimestamp*/ 100);
    auto msg2 = MakeMessage("msg-2", /*systemTimestamp*/ 100);

    store->Register({msg1});
    SyncStore(store);

    // msg1 is in the table → Processed; msg2 is not → Unprocessed.
    auto result = store->Filter({msg1, msg2}, /*checkState*/ true);

    ASSERT_EQ(std::ssize(result.Processed), 1);
    ASSERT_EQ(std::ssize(result.Unprocessed), 1);
    EXPECT_EQ(result.Processed[0]->MessageId, TMessageId("msg-1"));
    EXPECT_EQ(result.Unprocessed[0]->MessageId, TMessageId("msg-2"));
}

// Verifies that messages with the same MessageId under different ComputationIds
// are stored independently — registering one does not affect the other.
TEST_W(TTestInputStore, SameMessageIdDifferentComputationIdAreIndependent)
{
    // Two contexts with different ComputationIds sharing the same pipeline path.
    auto contextA = PrepareInputStoreContext();
    auto contextB = PrepareInputStoreContext();
    // Give them distinct ComputationIds.
    contextA->Partition->ComputationId = TComputationId("comp-A");
    contextB->Partition->ComputationId = TComputationId("comp-B");

    auto storeA = CreateInputStore(contextA, New<TDynamicInputStoreSpec>());
    auto storeB = CreateInputStore(contextB, New<TDynamicInputStoreSpec>());
    WaitFor(storeA->Init()).ThrowOnError();
    WaitFor(storeB->Init()).ThrowOnError();

    auto msg = MakeMessage("shared-id", /*systemTimestamp*/ 100);

    // Register and sync only under comp-A.
    storeA->Register({msg});
    SyncStore(storeA);

    // comp-A sees it as Processed; comp-B does not.
    auto resultA = storeA->Filter({msg}, /*checkState*/ true);
    ASSERT_EQ(std::ssize(resultA.Processed), 1);
    EXPECT_EQ(resultA.Processed[0]->MessageId, TMessageId("shared-id"));

    auto resultB = storeB->Filter({msg}, /*checkState*/ true);
    ASSERT_EQ(std::ssize(resultB.Unprocessed), 1);
    EXPECT_EQ(resultB.Unprocessed[0]->MessageId, TMessageId("shared-id"));
}

// Verifies that Init() loads previously persisted messages and Filter() sees them.
TEST_W(TTestInputStore, InitAndFilter)
{
    auto context = PrepareInputStoreContext();

    // First store instance: register and sync.
    {
        auto store = CreateInputStore(context, New<TDynamicInputStoreSpec>());
        WaitFor(store->Init()).ThrowOnError();

        auto msg = MakeMessage("persisted-msg");
        store->Register({msg});
        SyncStore(store);
    }

    // Second store instance on the same context: Init() must not fail,
    // and Filter() must report the previously written message as Processed.
    {
        auto store = CreateInputStore(context, New<TDynamicInputStoreSpec>());
        WaitFor(store->Init()).ThrowOnError();

        auto msg = MakeMessage("persisted-msg");
        auto result = store->Filter({msg}, /*checkState*/ true);

        ASSERT_EQ(std::ssize(result.Processed), 1);
        EXPECT_EQ(result.Processed[0]->MessageId, TMessageId("persisted-msg"));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
