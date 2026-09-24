#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/connectors/common/async_at_most_once_sink_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/sink_controller_base.h>

#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TAtMostOnceTestSinkController
    : public TSinkControllerBase
{
public:
    using TSinkControllerBase::TSinkControllerBase;

    std::optional<i64> GetReceiverChannelCount() override
    {
        return 1;
    }
};

class TAtMostOnceTestSink
    : public TAsyncAtMostOnceSinkBase
{
public:
    using TSinkController = TAtMostOnceTestSinkController;

    TAtMostOnceTestSink(
        TSinkContextPtr context,
        TDynamicSinkContextPtr dynamicContext)
        : TAsyncAtMostOnceSinkBase(std::move(context), std::move(dynamicContext))
    { }

    void SetPromises(std::shared_ptr<std::vector<TPromise<void>>> promises)
    {
        Promises_ = std::move(promises);
    }

private:
    std::shared_ptr<std::vector<TPromise<void>>> Promises_;
    int NextPromiseIndex_ = 0;

    void DoInit(const std::string& /*producerId*/) override
    { }

    std::pair<TFuture<void>, ui64> DoDistribute(
        const TOutputMessageConstPtr& /*message*/,
        i64 /*seqNo*/) override
    {
        return {Promises_->at(NextPromiseIndex_++).ToFuture(), 1};
    }
};

YT_FLOW_DEFINE_SINK(TAtMostOnceTestSink);

class TLifetimeProbe
    : public TRefCounted
{ };

////////////////////////////////////////////////////////////////////////////////

TEST(TAsyncAtMostOnceSinkTest, RetainsResourcesUntilAllAcceptedFuturesSet)
{
    auto actionQueue = New<TActionQueue>("AsyncAtMostOnceSinkTest");

    const TStreamId streamId("input");
    auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("value", EValueType::Int64),
    });
    auto streamSpec = New<TStreamSpec>();
    streamSpec->Schema = schema;
    THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>> streamSpecs;
    streamSpecs[streamId][TStreamSpecId(1)] = streamSpec;
    auto streamSpecStorage = New<TComputationStreamSpecStorage>(
        New<TStreamSpecs>(std::move(streamSpecs)),
        New<TTableSchema>(),
        /*evaluatorCache*/ nullptr);

    auto context = New<TSinkContext>();
    context->Logger = NLogging::TLogger("AsyncAtMostOnceSinkTest");
    context->SerializedInvoker = actionQueue->GetInvoker();
    context->StreamSpecStorage = streamSpecStorage;
    context->SinkSpec = New<TSinkSpec>();
    context->SinkSpec->SinkClassName = TypeName<TAtMostOnceTestSink>();
    context->SinkSpec->InputStreamIds = {streamId};

    auto dynamicContext = New<TDynamicSinkContext>();
    dynamicContext->DynamicSinkSpec = New<TDynamicSinkSpec>();

    auto promises = std::make_shared<std::vector<TPromise<void>>>(
        std::initializer_list<TPromise<void>>{NewPromise<void>(), NewPromise<void>()});
    auto sink = New<TAtMostOnceTestSink>(context, dynamicContext);
    sink->SetPromises(promises);
    auto parameters = New<TAtMostOnceStrategyDynamicParameters>();
    parameters->SuspendDestructionDuration = TDuration::Minutes(1);
    parameters->TotalQueueBytesLimit = NYTree::TSize(1_KB);
    sink->Reconfigure(parameters);
    sink->Init(/*initContext*/ nullptr);

    TMessageBuilder builder(streamId, schema);
    builder.SetMessageId(TMessageId("message"));
    builder.SetSystemTimestamp(TSystemTimestamp(1));
    builder.SetAlignmentTimestamp(TSystemTimestamp(1));
    builder.SetEventTimestamp(TSystemTimestamp(1));
    builder.Payload().Set<i64>(1, "value");
    auto message = New<TOutputMessage>(builder.Finish(), streamSpecStorage);

    sink->Distribute(message, TOnDistributedCallback::FromCallback([] {
    }));
    sink->Distribute(message, TOnDistributedCallback::FromCallback([] {
    }));
    sink->Commit();

    auto probe = New<TLifetimeProbe>();
    auto weakProbe = MakeWeak(probe);
    sink->SuspendDestructionGuarded({probe});
    probe.Reset();
    sink.Reset();

    EXPECT_FALSE(weakProbe.IsExpired());
    promises->at(0).Set(TError("Expected test failure"));
    EXPECT_FALSE(weakProbe.IsExpired());

    promises->at(1).Set();
    WaitFor(BIND([] {
    })
            .AsyncVia(actionQueue->GetInvoker())
            .Run())
        .ThrowOnError();
    EXPECT_TRUE(weakProbe.IsExpired());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
