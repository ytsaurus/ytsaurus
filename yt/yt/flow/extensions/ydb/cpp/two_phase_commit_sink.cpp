#include "two_phase_commit_sink.h"

#include "client_factory.h"
#include "deferred_publication.h"
#include "options.h"
#include "sink_engine.h"
#include "state.h"
#include "two_phase_commit_engine.h"

#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/flow/library/cpp/common/resource.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TYdbTwoPhaseCommitSinkController::TYdbTwoPhaseCommitSinkController(
    TSinkControllerContextPtr context,
    TDynamicSinkControllerContextPtr dynamicContext)
    : TYdbSinkController(std::move(context), std::move(dynamicContext))
    , OrderingWarningState_(GetContext()->StatusProfiler->ErrorState("/ordering_warning"))
{
    UpdateOrderingWarning();
}

void TYdbTwoPhaseCommitSinkController::Sync()
{
    TYdbSinkController::Sync();
    UpdateOrderingWarning();
}

void TYdbTwoPhaseCommitSinkController::UpdateOrderingWarning()
{
    const bool strict = GetContext()->ComputationSpec &&
        GetContext()->ComputationSpec->DistributionOrdering == EDistributionOrdering::Strict;
    if (strict && Engine_->IsFederated().value_or(false)) {
        OrderingWarningState_->SetError(TError(
            "Strict distribution ordering with federated topic publication may reorder visibility "
            "and unfinished interrupting partitions may block their successors; "
            "set distribution_ordering=\"relaxed\""));
    } else {
        OrderingWarningState_->ClearError();
    }
}

////////////////////////////////////////////////////////////////////////////////

TYdbTwoPhaseCommitSink::TYdbTwoPhaseCommitSink(
    TSinkContextPtr context,
    TDynamicSinkContextPtr dynamicContext)
    : TSinkBase(std::move(context), std::move(dynamicContext))
    , Engine_([&] {
        auto options = MakeYdbTwoPhaseCommitOptions(*GetParameters());
        auto clientFactory = GetContext()->GetStaticResource(YdbDriverDefaultResourceId)->As<IYdbClientFactory>();
        auto writer = New<TYdbSinkEngine>(
            GetContext(),
            std::make_shared<const TYdbSinkOptions>(options->Sink),
            clientFactory,
            GetContext()->StatusProfiler,
            TSinkBase::Logger);
        auto publication = New<TYdbDeferredPublication>(
            GetContext(),
            clientFactory,
            options->WriterIdentityType,
            CreateYdbStateAccess<TYdbDeferredPublicationState, TYdbPublicationSnapshot>("v0"));
        return New<TYdbTwoPhaseCommitEngine>(
            GetContext(),
            std::move(options),
            MakeYdbTwoPhaseCommitDynamicOptions(*GetDynamicParameters()),
            std::move(writer),
            std::move(publication),
            CreateYdbStateAccess<TYdbTwoPhaseCommitSinkState, TYdbSinkCheckpoint>("v0"));
    }())
{
    Engine_->ValidateInput(TypeName(*this));
    SubscribeReconfigured(BIND([
        weakEngine = MakeWeak(Engine_),
        sinkSpec = GetContext()->SinkSpec,
        invoker = GetContext()->SerializedInvoker
    ] (const TDynamicSinkContextPtr& incoming) {
        auto parsed = DynamicPointerCast<TDynamicYdbTwoPhaseCommitSinkParameters>(
            TRegistry::Get()->ParseDynamicSinkParameters(sinkSpec, incoming->DynamicSinkSpec));
        auto options = MakeYdbTwoPhaseCommitDynamicOptions(*parsed);
        YT_UNUSED_FUTURE(BIND([weakEngine, options = std::move(options)] {
            if (auto engine = weakEngine.Lock()) {
                engine->Reconfigure(options);
            }
        })
                .AsyncVia(invoker)
                .Run());
    }));
}

void TYdbTwoPhaseCommitSink::Init(IInitContextPtr initContext)
{
    Engine_->Init(std::move(initContext));
}

void TYdbTwoPhaseCommitSink::Distribute(const TOutputMessageConstPtr& message, TOnDistributedCallback onDistributed)
{
    Engine_->Distribute(message, std::move(onDistributed));
}

void TYdbTwoPhaseCommitSink::PreSyncFlush()
{
    Engine_->PreSyncFlush([this] (const TStreamId& streamId, TSystemTimestamp eventTimestamp) {
        ObserveEventLag(streamId, eventTimestamp);
    });
}

void TYdbTwoPhaseCommitSink::Sync(NApi::IDynamicTableTransactionPtr /*transaction*/)
{
    Engine_->Sync();
}

void TYdbTwoPhaseCommitSink::Commit()
{
    Engine_->Commit();
}

THashMap<TStreamId, TInflightStreamTraverseDataPtr> TYdbTwoPhaseCommitSink::BuildInflight() const
{
    return Engine_->BuildInflight();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
