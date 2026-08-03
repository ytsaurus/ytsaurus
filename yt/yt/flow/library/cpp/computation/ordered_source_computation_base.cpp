#include "ordered_source_computation_base.h"

#include "message_filter.h"

#include <yt/yt/flow/library/cpp/connectors/common/ordered_source.h>

#include <algorithm>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TOrderedSourceComputationBase::TOrderedSourceComputationBase(
    TComputationContextPtr context,
    TDynamicComputationContextPtr dynamicContext)
    : TUniversalComputationBase(std::move(context), std::move(dynamicContext))
{
    if (GetContext()->Partition->State == EPartitionState::Executing) {
        if (!ActiveSource_) {
            THROW_ERROR_EXCEPTION("Active source is undefined");
        }
        if (!ActiveSourceStreamId_) {
            THROW_ERROR_EXCEPTION("Active source streamId is undefined");
        }
        if (!GetContext()->Partition->SourceKey) {
            THROW_ERROR_EXCEPTION("Source Key is undefined");
        }
        OrderedSource_ = DynamicPointerCast<IOrderedSource>(ActiveSource_);
        THROW_ERROR_EXCEPTION_UNLESS(OrderedSource_, "Expected IOrderedSource for source %Qv in computation %Qv", ActiveSourceStreamId_, GetComputationId());
    }
    Filter_ = CreateMessageFilter(GetDynamicSpec()->SkipIfExpression);
    SkippedByExpressionCounter_ = GetContext()->Profiler.WithPrefix("/source_streams").Counter("/skipped_by_expression_count");
    SubscribeOnReconfigure(
        BIND([this] {
            Filter_->Reconfigure(GetDynamicSpec()->SkipIfExpression);
        }),
        EWatchReconfigure::DynamicComputationSpec);
}

void TOrderedSourceComputationBase::DoPrepare(const IComputationRunContextPtr& context)
{
    InitOutputStoreDistribution(context);
}

TComputationOrchidStatePtr TOrderedSourceComputationBase::GetOrchidState()
{
    auto state = TUniversalComputationBase::GetOrchidState();
    auto universalState = DynamicPointerCast<TUniversalComputationOrchidState>(state);
    YT_VERIFY(universalState);
    universalState->PartitionDescription = Format("SourceKey: %v", *GetContext()->Partition->SourceKey);
    return universalState;
}

void TOrderedSourceComputationBase::DoInit(IJobInitContextPtr /*initContext*/)
{
    DoInit();
}

void TOrderedSourceComputationBase::DoInit()
{ }

void TOrderedSourceComputationBase::DoProcess(IInputContextPtr input, IOutputCollectorPtr output)
{
    YT_VERIFY(input->GetTimers().empty());
    std::vector<TInputMessageConstPtr> parents(1);
    for (const auto& message : input->GetMessages()) {
        parents[0] = message;
        DoProcessMessage(message, output->SetParents(parents, {}, {}));
    }
}

void TOrderedSourceComputationBase::DoProcessMessage(const TInputMessageConstPtr& message, IOutputCollectorPtr output)
{
    DoProcessMessage(*message, std::move(output));
}

void TOrderedSourceComputationBase::DoProcessMessage(const TMessage& /*message*/, IOutputCollectorPtr /*output*/)
{ }

void TOrderedSourceComputationBase::DoSync(IRetryableTransactionPtr /*transaction*/)
{ }

void TOrderedSourceComputationBase::ValidateOrderedSourceSpec(const TComputationSpec& spec, TStringBuf className)
{
    if (!spec.InputStreamIds.empty()) {
        THROW_ERROR_EXCEPTION("%v does not support input streams", className);
    }
    if (!spec.TimerStreams.empty()) {
        THROW_ERROR_EXCEPTION("%v does not support timers", className);
    }
    if (!spec.KeyVisitorStreams.empty()) {
        THROW_ERROR_EXCEPTION("%v does not support key_visitor_streams", className);
    }
    if (!spec.ExternalStateManagers.empty()) {
        THROW_ERROR_EXCEPTION("%v does not support external_state_managers", className);
    }
    for (const auto& [name, joinerSpec] : spec.ExternalStateJoiners) {
        if (!joinerSpec->JoinOn->KeySchemaOverride) {
            THROW_ERROR_EXCEPTION("%v does not support external_state_joiner %Qv without join_on/key_schema_override",
                className,
                name);
        }
    }
}

TSystemTimestamp TOrderedSourceComputationBase::GetReadDelayThreshold()
{
    auto threshold = InfinitySystemTimestamp;
    if (GetSpec()->WatermarkStrategy->WatermarkAlignment && GetSpec()->WatermarkStrategy->WatermarkAlignment->ReadDelays) {
        for (const auto& [streamId, delay] : *GetSpec()->WatermarkStrategy->WatermarkAlignment->ReadDelays) {
            auto watermark = GetEpochEventWatermark(streamId);
            threshold = std::min(threshold, TSystemTimestamp(std::max(watermark.Underlying(), delay.Seconds()) - delay.Seconds()));
        }
    }
    return threshold;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
