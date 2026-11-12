#ifndef JOB_IO_METER_INL_H_
#error "Direct inclusion of this file is not allowed, include job_io_meter.h"
// For the sake of sane code completion.
#include "job_io_meter.h"
#endif

#include "chunk_reader_options.h"

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

template <class TRequestPtr, class TClientChunkOptions>
void SetRequestIoConsumed(
    const TRequestPtr& req,
    const TClientChunkOptions& options)
{
    if (const auto& jobIoMeter = options.JobIoMeter; jobIoMeter && jobIoMeter->IsEnabled()) {
        req->set_io_consumed(jobIoMeter->GetIoConsumedInWindow());
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

template <class TRequestPtr>
void SetRequestReadIoFairShareState(
    const TRequestPtr& req,
    const TClientChunkReadOptions& options,
    std::optional<double> configuredWeight)
{
    if (const auto& state = options.ForwardedIoFairShareState) {
        if (state->IoConsumed) {
            req->set_io_consumed(*state->IoConsumed);
        }
        if (state->IoFairShareWeight) {
            req->set_io_fair_share_weight(*state->IoFairShareWeight);
        }
        return;
    }

    SetRequestIoConsumed(req, options);
    SetRequestIoFairShareWeight(req, options, configuredWeight);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
