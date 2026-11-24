#include "sink.h"

#include "client_factory.h"
#include "options.h"
#include "sink_engine.h"
#include "state.h"

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/flow/library/cpp/common/resource.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

namespace {

template <class TSinkContextPtr>
IYdbClientFactoryPtr GetClientFactory(const TSinkContextPtr& context)
{
    return context->GetStaticResource(YdbDriverDefaultResourceId)->template As<IYdbClientFactory>();
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TYdbSink::TYdbSink(
    TSinkContextPtr context,
    TDynamicSinkContextPtr dynamicContext)
    : TOrderedAsyncSinkBase(std::move(context), std::move(dynamicContext))
    , Engine_(New<TYdbSinkEngine>(
        GetContext(),
        MakeYdbSinkOptions(*GetParameters(), /*framingFormat*/ std::nullopt),
        GetClientFactory(GetContext()),
        GetContext()->StatusProfiler,
        TOrderedAsyncSinkBase::Logger))
{
    Engine_->ValidatePayloadColumn(TypeName(*this));
}

void TYdbSink::DoInit(const std::string& producerId)
{
    Engine_->InitSession(producerId);
}

TFuture<void> TYdbSink::DoDistribute(const TOutputMessageConstPtr& message, i64 seqNo)
{
    return Engine_->WriteMessage(seqNo, *message);
}

////////////////////////////////////////////////////////////////////////////////

TYdbFramingSink::TYdbFramingSink(
    TSinkContextPtr context,
    TDynamicSinkContextPtr dynamicContext)
    : TOrderedBatchingAsyncSinkBase(std::move(context), std::move(dynamicContext))
    , Engine_(New<TYdbSinkEngine>(
        GetContext(),
        MakeYdbSinkOptions(*GetParameters(), GetParameters()->FramingFormat),
        GetClientFactory(GetContext()),
        GetContext()->StatusProfiler,
        TOrderedBatchingAsyncSinkBase::Logger))
{
    Engine_->ValidatePayloadColumn(TypeName(*this));
}

void TYdbFramingSink::DoInit(const std::string& producerId)
{
    Engine_->InitSession(producerId);
}

TFuture<void> TYdbFramingSink::DoDistribute(const std::vector<TOutputMessageConstPtr>& messages, i64 seqNo)
{
    return Engine_->WriteFramingBatch(seqNo, messages);
}

////////////////////////////////////////////////////////////////////////////////

TYdbAtLeastOnceSink::TYdbAtLeastOnceSink(
    TSinkContextPtr context,
    TDynamicSinkContextPtr dynamicContext)
    : TSyncSinkBase(std::move(context), std::move(dynamicContext))
    , Engine_(New<TYdbSinkEngine>(
        GetContext(),
        MakeYdbSinkOptions(*GetParameters(), /*framingFormat*/ std::nullopt),
        GetClientFactory(GetContext()),
        GetContext()->StatusProfiler,
        TSyncSinkBase::Logger))
{
    Engine_->ValidatePayloadColumn(TypeName(*this));
}

void TYdbAtLeastOnceSink::DoInit()
{
    Engine_->InitAtLeastOnceSession();
}

void TYdbAtLeastOnceSink::DoDistribute(NApi::IDynamicTableTransactionPtr /*transaction*/, const std::deque<TOutputMessageConstPtr>& messages)
{
    Engine_->DistributeAtLeastOnce(messages, /*framingBatching*/ nullptr);
}

////////////////////////////////////////////////////////////////////////////////

TYdbAtLeastOnceFramingSink::TYdbAtLeastOnceFramingSink(
    TSinkContextPtr context,
    TDynamicSinkContextPtr dynamicContext)
    : TSyncSinkBase(std::move(context), std::move(dynamicContext))
    , Engine_(New<TYdbSinkEngine>(
        GetContext(),
        MakeYdbSinkOptions(*GetParameters(), GetParameters()->FramingFormat),
        GetClientFactory(GetContext()),
        GetContext()->StatusProfiler,
        TSyncSinkBase::Logger))
    , Batching_(MakeYdbBatchingOptions(GetDynamicParameters()->MaxRowsPerBatch, GetDynamicParameters()->MaxBytesPerBatch))
{
    Engine_->ValidatePayloadColumn(TypeName(*this));
    SubscribeReconfigured(BIND([
        weakThis = MakeWeak(this),
        sinkSpec = GetContext()->SinkSpec,
        invoker = GetContext()->SerializedInvoker
    ] (const TDynamicSinkContextPtr& incoming) {
        auto parsed = DynamicPointerCast<TDynamicYdbAtLeastOnceFramingSinkParameters>(
            TRegistry::Get()->ParseDynamicSinkParameters(sinkSpec, incoming->DynamicSinkSpec));
        auto batching = MakeYdbBatchingOptions(parsed->MaxRowsPerBatch, parsed->MaxBytesPerBatch);
        YT_UNUSED_FUTURE(BIND([weakThis, batching = std::move(batching)] {
            if (auto sink = weakThis.Lock()) {
                sink->Batching_ = batching;
            }
        })
                .AsyncVia(invoker)
                .Run());
    }));
}

void TYdbAtLeastOnceFramingSink::DoInit()
{
    Engine_->InitAtLeastOnceSession();
}

void TYdbAtLeastOnceFramingSink::DoDistribute(NApi::IDynamicTableTransactionPtr /*transaction*/, const std::deque<TOutputMessageConstPtr>& messages)
{
    const auto batching = Batching_;
    Engine_->DistributeAtLeastOnce(messages, batching.get());
}

////////////////////////////////////////////////////////////////////////////////

TYdbSinkController::TYdbSinkController(
    TSinkControllerContextPtr context,
    TDynamicSinkControllerContextPtr dynamicContext)
    : TSinkControllerBase(std::move(context), std::move(dynamicContext))
    , Engine_(New<TYdbSinkControllerEngine>(
        GetContext(),
        MakeYdbTopologyOptions(*GetParameters()),
        CreateYdbStateAccess<TYdbTopicInfoControllerState, TYdbTopologySnapshot>("v0"),
        CreateYdbStateAccess<TYdbHeartbeatState, TYdbHeartbeatSnapshot>("heartbeat"),
        MakeYdbSinkControllerDynamicOptions(*GetDynamicParameters()),
        GetParameters()->WriteFlowQueueMeta,
        GetParameters()->FlowQueueMetaName,
        GetClientFactory(GetContext()),
        BIND([weakThis = MakeWeak(this)] () -> TWatermarkStatePtr {
            auto controller = weakThis.Lock();
            return controller ? controller->GetWatermarkState() : nullptr;
        }),
        Logger,
        "/ydb_info"))
{
    SubscribeReconfigured(BIND([
        weakEngine = MakeWeak(Engine_),
        sinkSpec = GetContext()->SinkSpec,
        invoker = GetContext()->Invoker
    ] (const TDynamicSinkControllerContextPtr& incoming) {
        auto parsed = DynamicPointerCast<TDynamicYdbSinkControllerParameters>(
            TRegistry::Get()->ParseDynamicSinkParameters(sinkSpec, incoming->DynamicSinkSpec));
        auto options = MakeYdbSinkControllerDynamicOptions(*parsed);
        YT_UNUSED_FUTURE(BIND([weakEngine, options = std::move(options)] {
            if (auto engine = weakEngine.Lock()) {
                engine->Reconfigure(options);
            }
        })
                .AsyncVia(invoker)
                .Run());
    }));
}

void TYdbSinkController::Init(IInitContextPtr initContext)
{
    Engine_->Init(std::move(initContext));
}

void TYdbSinkController::Sync()
{
    Engine_->Sync();
}

void TYdbSinkController::Commit()
{
    Engine_->Commit();
}

std::optional<i64> TYdbSinkController::GetReceiverChannelCount()
{
    return Engine_->GetReceiverChannelCount();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
