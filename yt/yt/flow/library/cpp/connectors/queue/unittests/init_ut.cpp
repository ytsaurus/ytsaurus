#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/connectors/queue/sink.h>
#include <yt/yt/flow/library/cpp/connectors/queue/tablet_router.h>

#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>
#include <yt/yt/flow/library/cpp/common/unittests/mock/state.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/cache/cache.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/unittests/mock/client.h>
#include <yt/yt/client/unittests/mock/transaction.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/suspendable_invoker.h>
#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/convert.h>

#include <atomic>

namespace NYT::NFlow {
namespace {

using namespace NApi;
using namespace NTableClient;
using namespace NYson;
using namespace NYTree;
using namespace testing;

////////////////////////////////////////////////////////////////////////////////

class TFixedClientsCache
    : public NClient::NCache::IClientsCache
{
public:
    explicit TFixedClientsCache(IClientPtr client)
        : Client_(std::move(client))
    { }

    IClientPtr GetClient(TStringBuf /*clusterUrl*/) override
    {
        return Client_;
    }

private:
    const IClientPtr Client_;
};

TEST(TSyncQueueSinkInitTest, ResolvesTabletCountOnFirstNonemptySync)
{
    const TStreamId streamId("input");
    auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("tablet", EValueType::Uint64),
        TColumnSchema("value", EValueType::String),
    });
    auto streamSpec = New<TStreamSpec>();
    streamSpec->Schema = schema;
    THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>> streamSpecs;
    streamSpecs[streamId][TStreamSpecId(1)] = streamSpec;

    auto queue = New<NConcurrency::TActionQueue>("QueueSinkInitTest");
    std::atomic<int> getNodeCalls = 0;
    auto client = New<StrictMock<TMockClient>>();
    EXPECT_CALL(*client, GetNode("//queue/@tablet_count", _))
        .WillRepeatedly([&] {
            ++getNodeCalls;
            return MakeFuture(ConvertToYsonString(4));
        });
    auto context = New<TSinkContext>();
    context->SinkSpec = ConvertTo<TSinkSpecPtr>(TYsonStringBuf(R"({
        sink_class_name = "NYT::NFlow::TSyncQueueSink";
        input_stream_ids = [input];
        parameters = {
            queue_path = "<cluster=test>//queue";
            tablet_index_expression = "tablet";
        };
    })"));
    context->ClientsCache = New<TFixedClientsCache>(client);
    context->PipelinePath = NYPath::TRichYPath("<cluster=test>//pipeline");
    context->PoolInvoker = queue->GetInvoker();
    auto streamSpecStorage = New<TComputationStreamSpecStorage>(
        New<TStreamSpecs>(std::move(streamSpecs)),
        New<TTableSchema>(),
        /*evaluatorCache*/ nullptr);
    context->StreamSpecStorage = streamSpecStorage;
    context->Logger = NLogging::TLogger("SyncQueueSinkInitTest");
    context->StatusProfiler = CreateSyncStatusProfiler();

    auto dynamicContext = New<TDynamicSinkContext>();
    dynamicContext->DynamicSinkSpec = ConvertTo<TDynamicSinkSpecPtr>(TYsonStringBuf("{}"));

    auto sink = New<TSyncQueueSink>(context, dynamicContext);
    auto stateManager = New<TStateManagerMock>();
    EXPECT_NO_THROW(sink->Init(stateManager->CreateContext()));
    EXPECT_EQ(getNodeCalls.load(), 0);

    auto transaction = New<NiceMock<TMockTransaction>>();
    sink->Sync(transaction);
    EXPECT_EQ(getNodeCalls.load(), 0);

    auto makeMessage = [&] (TStringBuf messageId) {
        TMessageBuilder builder(streamId, schema);
        builder.SetMessageId(TMessageId(messageId));
        builder.SetSystemTimestamp(TSystemTimestamp(1));
        builder.SetEventTimestamp(TSystemTimestamp(1));
        builder.SetAlignmentTimestamp(TSystemTimestamp(1));
        builder.Payload().Set<ui64>(2, "tablet");
        builder.Payload().Set<std::string>("value", "value");
        return New<TOutputMessage>(builder.Finish(), streamSpecStorage);
    };

    bool distributed = false;
    sink->Distribute(
        makeMessage("message-1"),
        TOnDistributedCallback::FromCallback(BIND([&] {
            distributed = true;
        })));
    EXPECT_TRUE(distributed);

    sink->Sync(transaction);
    EXPECT_EQ(getNodeCalls.load(), 1);

    sink->Distribute(
        makeMessage("message-2"),
        TOnDistributedCallback::FromCallback(BIND([&] {
            distributed = true;
        })));
    sink->Sync(transaction);
    EXPECT_EQ(getNodeCalls.load(), 1);
}

TEST(TTabletRouterTest, DefersAndStartsPeriodicRefresh)
{
    auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("tablet", EValueType::Uint64),
    });
    auto queue = New<NConcurrency::TActionQueue>("TabletRouterTest");
    auto suspendedInvoker = NConcurrency::CreateSuspendableInvoker(queue->GetInvoker());
    NConcurrency::WaitFor(suspendedInvoker->Suspend()).ThrowOnError();
    auto getNodeCalls = std::make_shared<std::atomic<int>>(0);
    auto refreshed = NewPromise<void>();
    auto client = New<NiceMock<TMockClient>>();
    ON_CALL(*client, GetNode("//queue/@tablet_count", _))
        .WillByDefault([getNodeCalls, refreshed] {
            if (++*getNodeCalls == 2) {
                refreshed.TrySet();
            }
            return MakeFuture(ConvertToYsonString(4));
        });

    auto context = New<TSinkContext>();
    context->ClientsCache = New<TFixedClientsCache>(client);
    context->PoolInvoker = suspendedInvoker;

    auto parameters = New<TQueueSinkTabletRoutingParameters>();
    parameters->TabletIndexExpression = "tablet";
    auto router = CreateTabletRouter(
        *parameters,
        NYPath::TRichYPath("<cluster=test>//queue"),
        TDuration::Hours(1),
        schema,
        context,
        NLogging::TLogger("TabletRouterTest"));

    router->Start();

    NConcurrency::WaitFor(NConcurrency::TDelayedExecutor::MakeDelayed(TDuration::MilliSeconds(10)))
        .ThrowOnError();
    suspendedInvoker->Resume();
    auto invokerDrained = NewPromise<void>();
    queue->GetInvoker()->Invoke(BIND([invokerDrained] {
        invokerDrained.Set();
    }));
    NConcurrency::WaitFor(invokerDrained.ToFuture()).ThrowOnError();
    EXPECT_EQ(getNodeCalls->load(), 1);

    router.Reset();
    getNodeCalls->store(0);
    router = CreateTabletRouter(
        *parameters,
        NYPath::TRichYPath("<cluster=test>//queue"),
        TDuration::MilliSeconds(50),
        schema,
        context,
        NLogging::TLogger("TabletRouterTest"));
    router->Start();
    EXPECT_TRUE(NConcurrency::WaitFor(refreshed.ToFuture().WithTimeout(TDuration::Seconds(5))).IsOK());
    EXPECT_GE(getNodeCalls->load(), 2);

    router.Reset();
    queue->Shutdown(/*graceful*/ true);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
