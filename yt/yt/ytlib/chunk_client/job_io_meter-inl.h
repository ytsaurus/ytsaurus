#ifndef JOB_IO_METER_INL_H_
#error "Direct inclusion of this file is not allowed, include job_io_meter.h"
// For the sake of sane code completion.
#include "job_io_meter.h"
#endif

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

template <class TRequestPtr, class TClientChunkOptions>
void SetRequestIoConsumed(
    const TRequestPtr& req,
    const TClientChunkOptions& options,
    TDuration window)
{
    if (const auto& jobIoMeter = options.JobIoMeter; jobIoMeter && jobIoMeter->IsEnabled()) {
        req->set_io_consumed(jobIoMeter->GetIoConsumedInWindow(window));
    }
}

template <class TRequestPtr, class TClientChunkOptions>
void SetRequestIoFairShareWeight(
    const TRequestPtr& req,
    const TClientChunkOptions& options,
    std::optional<double> configuredWeight)
{
    if (auto weight = GetEffectiveIoFairShareWeight(configuredWeight, options.JobIoMeter)) {
        req->set_io_fair_share_weight(*weight);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
