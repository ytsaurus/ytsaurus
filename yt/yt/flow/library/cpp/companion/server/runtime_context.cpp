#include "runtime_context.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>

namespace NYT::NFlow::NCompanionServer {

////////////////////////////////////////////////////////////////////////////////

void TCompanionRuntimeContext::RefreshBatchState(
    const TBatchInput& input,
    NYTree::IMapNodePtr dynamicParametersNode)
{
    HasCurrentTimestamp_ = input.CurrentTimestamp.has_value();
    // An absent sequence number keeps GetEpochUniqueSeqNo() throwing, as for in-process
    // hosts that publish none.
    RefreshEpochState(
        BuildWatermarkState(input.Watermarks, input.CurrentTimestamp),
        std::move(dynamicParametersNode),
        input.EpochUniqueSeqNo);
}

NConcurrency::IThroughputThrottlerPtr TCompanionRuntimeContext::GetThrottlerOrThrow(
    const TThrottlerId& throttlerId)
{
    THROW_ERROR_EXCEPTION("Distributed throttler %Qv is not available in a companion process",
        throttlerId);
}

NConcurrency::IThroughputThrottlerPtr TCompanionRuntimeContext::TryGetThrottler(
    const TThrottlerId& throttlerId)
{
    return GetThrottlerOrThrow(throttlerId);
}

TSystemTimestamp TCompanionRuntimeContext::GetCurrentTimestamp() const
{
    THROW_ERROR_EXCEPTION_UNLESS(HasCurrentTimestamp_,
        "The epoch timestamp was not sent to the companion process");
    return TComputationRuntimeContext::GetCurrentTimestamp();
}

////////////////////////////////////////////////////////////////////////////////

TWatermarkStatePtr BuildWatermarkState(
    const THashMap<TStreamId, TSystemTimestamp>& watermarks,
    std::optional<TSystemTimestamp> currentTimestamp)
{
    auto state = New<TWatermarkState>();
    if (currentTimestamp) {
        state->CurrentTimestamp = *currentTimestamp;
    }
    for (const auto& [streamId, watermark] : watermarks) {
        auto watermarks = New<TWatermarks>();
        // The wire carries only the event watermark; the system watermark stays
        // zero ("unknown") rather than guessed.
        watermarks->EventWatermark = watermark;
        state->Streams[streamId] = std::move(watermarks);
    }
    return state;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionServer
