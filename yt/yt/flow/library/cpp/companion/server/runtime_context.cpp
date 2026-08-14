#include "runtime_context.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>

namespace NYT::NFlow::NCompanionServer {

////////////////////////////////////////////////////////////////////////////////

NConcurrency::IThroughputThrottlerPtr TCompanionRuntimeContext::GetThrottler(
    const TThrottlerId& throttlerId)
{
    THROW_ERROR_EXCEPTION("Distributed throttler %Qv is not available in a companion process",
        throttlerId);
}

TSystemTimestamp TCompanionRuntimeContext::GetCurrentTimestamp() const
{
    THROW_ERROR_EXCEPTION("The epoch timestamp is not available in a companion process");
}

TUniqueSeqNo TCompanionRuntimeContext::GetEpochUniqueSeqNo() const
{
    THROW_ERROR_EXCEPTION("The epoch sequence number is not available in a companion process");
}

////////////////////////////////////////////////////////////////////////////////

TWatermarkStatePtr BuildWatermarkState(const THashMap<TStreamId, TSystemTimestamp>& watermarks)
{
    auto state = New<TWatermarkState>();
    for (const auto& [streamId, watermark] : watermarks) {
        auto watermarks = New<TWatermarks>();
        // The wire carries only the event watermark; the system watermark and
        // the epoch timestamp stay zero ("unknown") rather than guessed.
        watermarks->EventWatermark = watermark;
        state->Streams[streamId] = std::move(watermarks);
    }
    return state;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionServer
